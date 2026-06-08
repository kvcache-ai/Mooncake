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
    explicit LocalTransferAdmissionQueue(QueueLimits limits);

    LocalTransferAdmissionQueue(const LocalTransferAdmissionQueue&) = delete;
    LocalTransferAdmissionQueue& operator=(const LocalTransferAdmissionQueue&) =
        delete;
    LocalTransferAdmissionQueue(LocalTransferAdmissionQueue&&) = delete;
    LocalTransferAdmissionQueue& operator=(LocalTransferAdmissionQueue&&) =
        delete;

    Status tryAdmit(const QueueSubmit& submit,
                    std::vector<QueueOwnerId>& admitted_owner_ids);

    std::vector<QueueOwnerId> pickForDispatch(size_t max_owners,
                                              size_t max_bytes);

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
        Completed,
        Failed,
    };

    struct QueueOwner {
        uint64_t batch_token{0};
        Request request{};
        QueueOwnerKind kind{QueueOwnerKind::User};
        QueueState state{QueueState::Queued};
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
};

}  // namespace tent
}  // namespace mooncake

#endif  // ADMISSION_QUEUE_H_
