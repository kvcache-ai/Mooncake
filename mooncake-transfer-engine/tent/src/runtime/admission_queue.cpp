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

namespace mooncake {
namespace tent {

LocalTransferAdmissionQueue::LocalTransferAdmissionQueue(QueueLimits limits)
    : limits_(limits) {}

Status LocalTransferAdmissionQueue::tryAdmit(
    const QueueSubmit& submit, std::vector<QueueOwnerId>& admitted_owner_ids) {
    admitted_owner_ids.clear();
    if (submit.batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }
    if (!submit.owners.empty()) {
        return Status::NotImplemented("queue admission is not wired" LOC_MARK);
    }
    return Status::OK();
}

std::vector<QueueOwnerId> LocalTransferAdmissionQueue::pickForDispatch(
    size_t max_owners, size_t max_bytes) {
    (void)max_owners;
    (void)max_bytes;
    return {};
}

Status LocalTransferAdmissionQueue::complete(
    QueueOwnerId owner_id, TransferStatusEnum terminal_status) {
    (void)terminal_status;
    if (owner_id == 0) {
        return Status::InvalidArgument("invalid queue owner id" LOC_MARK);
    }
    return Status::NotImplemented("queue completion is not wired" LOC_MARK);
}

Status LocalTransferAdmissionQueue::retireBatch(uint64_t batch_token) {
    if (batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }
    return Status::OK();
}

Status LocalTransferAdmissionQueue::resolveOwner(uint64_t batch_token,
                                                 size_t public_task_id,
                                                 QueueOwnerId& owner_id) const {
    (void)public_task_id;
    owner_id = 0;
    if (batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }
    return Status::InvalidEntry("public task id not found" LOC_MARK);
}

Status LocalTransferAdmissionQueue::getPublicStatus(
    uint64_t batch_token, size_t public_task_id,
    TransferStatusEnum& status) const {
    (void)public_task_id;
    status = TransferStatusEnum::INVALID;
    if (batch_token == 0) {
        return Status::InvalidArgument("invalid batch token" LOC_MARK);
    }
    return Status::InvalidEntry("public task id not found" LOC_MARK);
}

size_t LocalTransferAdmissionQueue::outstandingOwners() const {
    return outstanding_owners_;
}

size_t LocalTransferAdmissionQueue::outstandingBytes() const {
    return outstanding_bytes_;
}

}  // namespace tent
}  // namespace mooncake
