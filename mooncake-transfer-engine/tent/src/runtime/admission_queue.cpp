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
#include <set>
#include <utility>

namespace mooncake {
namespace tent {
namespace {

using PublicTaskKey = std::pair<uint64_t, size_t>;

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

    std::set<PublicTaskKey> public_keys;
    size_t byte_charge = 0;
    size_t user_owner_charge = 0;
    size_t user_byte_charge = 0;

    for (const auto& owner : submit.owners) {
        if (!isSupportedOwnerKind(owner.kind)) {
            return Status::InvalidArgument(
                "unsupported queue owner kind" LOC_MARK);
        }
        if (owner.request.length == 0) {
            return Status::InvalidArgument("empty transfer request" LOC_MARK);
        }

        const PublicTaskKey owner_key{submit.batch_token, owner.owner_task_id};
        if (!public_keys.insert(owner_key).second) {
            return Status::InvalidArgument("duplicate public task id" LOC_MARK);
        }
        for (const auto derived_task_id : owner.derived_task_ids) {
            if (derived_task_id == owner.owner_task_id) {
                return Status::InvalidArgument(
                    "owner task id appears in derived task ids" LOC_MARK);
            }
            const PublicTaskKey derived_key{submit.batch_token,
                                            derived_task_id};
            if (!public_keys.insert(derived_key).second) {
                return Status::InvalidArgument(
                    "duplicate public task id" LOC_MARK);
            }
        }

        CHECK_STATUS(
            checkedAdd(byte_charge, owner.request.length, byte_charge));
        if (owner.kind == QueueOwnerKind::User) {
            CHECK_STATUS(checkedAdd(user_owner_charge, 1, user_owner_charge));
            CHECK_STATUS(checkedAdd(user_byte_charge, owner.request.length,
                                    user_byte_charge));
        }
    }

    if (public_keys.size() > submit.batch_slots_left) {
        return Status::TooManyRequests(
            "batch public task capacity exceeded" LOC_MARK);
    }

    for (const auto& key : public_keys) {
        if (public_to_owner_.count(key)) {
            return Status::InvalidEntry(
                "public task id already admitted" LOC_MARK);
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
    for (const auto& owner_input : submit.owners) {
        const QueueOwnerId owner_id = next_owner_id_++;
        QueueOwner owner;
        owner.batch_token = submit.batch_token;
        owner.request = owner_input.request;
        owner.kind = owner_input.kind;
        owner.degradation_eligible = owner_input.degradation_eligible;
        owners_.emplace(owner_id, owner);

        public_to_owner_[{submit.batch_token, owner_input.owner_task_id}] =
            owner_id;
        for (const auto derived_task_id : owner_input.derived_task_ids) {
            public_to_owner_[{submit.batch_token, derived_task_id}] = owner_id;
        }
        // RFC #2519 step 2: keep fifo_ ordered on admission so pickForDispatch
        // never has to re-sort. Default (deadline_aware == false) appends in
        // strict FIFO. When deadline-aware, insert at the earliest-deadline-
        // first position; upper_bound places a new owner *after* existing
        // owners with the same deadline, preserving FIFO order among ties.
        if (limits_.deadline_aware) {
            const uint64_t key = deadlineKey(owner.request.deadline_ns);
            auto pos = std::upper_bound(
                fifo_.begin(), fifo_.end(), key,
                [this](uint64_t k, QueueOwnerId id) {
                    auto it = owners_.find(id);
                    uint64_t d =
                        (it == owners_.end())
                            ? std::numeric_limits<uint64_t>::max()
                            : deadlineKey(it->second.request.deadline_ns);
                    return k < d;
                });
            fifo_.insert(pos, owner_id);
        } else {
            fifo_.push_back(owner_id);
        }
        admitted_owner_ids.push_back(owner_id);
    }

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

    // RFC #2519 step 2 (opt-in): earliest-deadline-first dispatch. When
    // deadline_aware, fifo_ is kept EDF-ordered at admission time (see
    // tryAdmit's ordered insert), so there is nothing to sort here — we just
    // consume from the front. This keeps the hot dispatch path O(picked)
    // instead of re-sorting the whole queue on every call. Default
    // (deadline_aware == false) is plain FIFO.
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

    // Deadline proximity promotion: partition fifo_ so owners with critical
    // slack (deadline approaching within promotion_slack_ns) appear before
    // owners with comfortable slack or no deadline. stable_partition preserves
    // relative EDF order within each group.
    if (promotion_enabled) {
        std::stable_partition(fifo_.begin(), fifo_.end(), [&](QueueOwnerId id) {
            auto it = owners_.find(id);
            if (it == owners_.end() || it->second.state != QueueState::Queued) {
                return false;
            }
            const uint64_t dl = it->second.request.deadline_ns;
            if (dl == 0 || dl <= now_ns) return false;
            return (dl - now_ns) < limits_.promotion_slack_ns;
        });
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
    while (!fifo_.empty() && used_owners < max_owners) {
        auto owner_id = fifo_.front();
        auto owner_it = owners_.find(owner_id);
        // Non-queued entries should not normally remain in fifo_, but stale
        // entries are skipped defensively so retireBatch() does not need to
        // scan the dispatch queue.
        if (owner_it == owners_.end() ||
            owner_it->second.state != QueueState::Queued) {
            fifo_.pop_front();
            continue;
        }

        // Step 3: an owner predicted to miss its deadline is dropped (not
        // dispatched) and does not consume the dispatch budget. Because the
        // queue is EDF-ordered, later owners have looser deadlines, so we keep
        // scanning rather than stopping.
        if (shouldDrop(owner_it->second)) {
            fifo_.pop_front();
            dropOwner(owner_id, owner_it->second);
            continue;
        }

        const auto& owner = owner_it->second;
        const size_t remaining_bytes = max_bytes - used_bytes;
        if (owner.request.length > remaining_bytes) break;

        fifo_.pop_front();
        owner_it->second.state = QueueState::Dispatching;
        picked.push_back(owner_id);
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

    const PublicTaskKey batch_begin{batch_token, 0};
    auto public_begin = public_to_owner_.lower_bound(batch_begin);
    auto public_end = public_to_owner_.upper_bound(
        {batch_token, std::numeric_limits<size_t>::max()});

    std::set<QueueOwnerId> owner_ids;
    for (auto it = public_begin; it != public_end; ++it) {
        owner_ids.insert(it->second);
    }

    for (const auto owner_id : owner_ids) {
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

    for (const auto owner_id : owner_ids) {
        owners_.erase(owner_id);
    }
    public_to_owner_.erase(public_begin, public_end);
    return Status::OK();
}

Status LocalTransferAdmissionQueue::resolveOwner(uint64_t batch_token,
                                                 size_t public_task_id,
                                                 QueueOwnerId& owner_id) const {
    if (batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }
    auto it = public_to_owner_.find({batch_token, public_task_id});
    if (it == public_to_owner_.end()) {
        return Status::InvalidEntry("public task id not found" LOC_MARK);
    }
    owner_id = it->second;
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
