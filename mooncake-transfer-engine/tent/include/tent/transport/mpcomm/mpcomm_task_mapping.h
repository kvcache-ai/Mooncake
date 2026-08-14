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

#ifndef TENT_TRANSPORT_MPCOMM_MPCOMM_TASK_MAPPING_H
#define TENT_TRANSPORT_MPCOMM_MPCOMM_TASK_MAPPING_H

#include <cstddef>
#include <string>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/transport.h"
#include "tent/transport/mpcomm/mpcomm_adapter.h"

namespace mooncake {
namespace tent {

// Per-task state for tracking mpcomm async transfers
struct MpcommTask {
    Request request;
    volatile TransferStatusEnum status_word{TransferStatusEnum::PENDING};
    size_t transferred_bytes{0};
    MpcommTransferHandle mpcomm_handle{kInvalidMpcommTransferHandle};
};

// SubBatch implementation for mpcomm transport
struct MpcommSubBatch : public Transport::SubBatch {
    std::vector<MpcommTask> task_list;
    size_t max_size{0};
    size_t size() const override { return task_list.size(); }
};

// Issues one request on the adapter, returning the provider handle or
// kInvalidMpcommTransferHandle.
//
// This and pollMpcommTask() are free functions in their own translation unit
// rather than private methods so that the request/completion mapping can be
// driven against an injected adapter. That keeps the WRITE/READ mapping, the
// short-transfer guard and releasing each handle exactly once under test in a
// build that has neither RDMA hardware nor libmpcomm.
MpcommTransferHandle issueMpcommTransfer(MpcommAdapter &adapter,
                                         const Request &request,
                                         const std::string &host_id);

// Polls one task once and, if the transfer reached a terminal state, records
// the outcome and releases the handle. A task that is not pending, or that has
// no outstanding handle, is left untouched.
void pollMpcommTask(MpcommAdapter &adapter, MpcommTask &task);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_MPCOMM_MPCOMM_TASK_MAPPING_H
