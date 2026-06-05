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

#include <limits>
#include <set>

namespace mooncake {
namespace tent {
namespace {

using PublicTaskKey = std::pair<uint64_t, size_t>;

bool isSupportedTerminalStatus(TransferStatusEnum status) {
    return status == TransferStatusEnum::COMPLETED ||
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
    : limits_(limits) {}

Status LocalTransferAdmissionQueue::tryAdmit(
    const QueueSubmit& submit, std::vector<QueueOwnerId>& admitted_owner_ids) {
    admitted_owner_ids.clear();
    CHECK_STATUS(validateLimits(limits_));
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
        owners_.emplace(owner_id, owner);

        public_to_owner_[{submit.batch_token, owner_input.owner_task_id}] =
            owner_id;
        for (const auto derived_task_id : owner_input.derived_task_ids) {
            public_to_owner_[{submit.batch_token, derived_task_id}] = owner_id;
        }
        fifo_.push_back(owner_id);
        admitted_owner_ids.push_back(owner_id);
    }

    outstanding_owners_ = next_outstanding_owners;
    outstanding_bytes_ = next_outstanding_bytes;
    outstanding_user_owners_ = next_user_owners;
    outstanding_user_bytes_ = next_user_bytes;
    return Status::OK();
}

std::vector<QueueOwnerId> LocalTransferAdmissionQueue::pickForDispatch(
    size_t max_owners, size_t max_bytes) {
    std::vector<QueueOwnerId> picked;
    if (max_owners == 0 || max_bytes == 0) return picked;

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

    owner.state = terminal_status == TransferStatusEnum::COMPLETED
                      ? QueueState::Completed
                      : QueueState::Failed;
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

    for (const auto& entry : owners_) {
        const auto& owner = entry.second;
        const bool terminal = owner.state == QueueState::Completed ||
                              owner.state == QueueState::Failed;
        if (owner.batch_token == batch_token && !terminal) {
            return Status::InvalidEntry(
                "batch has non-terminal queue owners" LOC_MARK);
        }
    }

    for (auto it = owners_.begin(); it != owners_.end();) {
        if (it->second.batch_token == batch_token) {
            it = owners_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = public_to_owner_.begin(); it != public_to_owner_.end();) {
        if (it->first.first == batch_token) {
            it = public_to_owner_.erase(it);
        } else {
            ++it;
        }
    }
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
        case QueueState::Completed:
            status = TransferStatusEnum::COMPLETED;
            break;
        case QueueState::Failed:
            status = TransferStatusEnum::FAILED;
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
