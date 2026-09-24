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

#include "tent/transport/mpcomm/mpcomm_task_mapping.h"

#include <glog/logging.h>

namespace mooncake {
namespace tent {

MpcommTransferHandle issueMpcommTransfer(MpcommAdapter &adapter,
                                         const Request &request,
                                         const std::string &host_id) {
    // MPComm performs its own slicing and NIC/QP selection, so one request maps
    // onto exactly one provider transfer.
    auto local_addr = reinterpret_cast<uintptr_t>(request.source);
    auto remote_addr = static_cast<uintptr_t>(request.target_offset);
    if (request.opcode == Request::WRITE) {
        return adapter.putAsync(local_addr, host_id, remote_addr,
                                request.length);
    }
    return adapter.getAsync(local_addr, host_id, remote_addr, request.length);
}

void pollMpcommTask(MpcommAdapter &adapter, MpcommTask &task) {
    if (task.status_word != TransferStatusEnum::PENDING ||
        task.mpcomm_handle == kInvalidMpcommTransferHandle) {
        return;
    }
    if (!adapter.isTransferComplete(task.mpcomm_handle)) return;

    auto outcome = adapter.getTransferResult(task.mpcomm_handle);
    if (!outcome.ok) {
        LOG(WARNING) << "MpcommTransport: Transfer failed, provider status="
                     << outcome.native_status;
        task.status_word = TransferStatusEnum::FAILED;
        // The cached peer is deliberately kept even for connection errors:
        // MPComm has no disconnect API, so a second connect() to the same host
        // replaces its connection record wholesale, leaking the previous queue
        // pairs and blanking the remote memory keys that concurrent transfers
        // are still using. Recovering from a peer restart therefore needs
        // library support and is left out of this version.
    } else if (outcome.bytes_transferred < task.request.length) {
        // Defensive: MPComm only reports success once every chunk has
        // completed, so this is not expected to trigger. It is kept so that a
        // future change in the library's success semantics cannot silently
        // surface a partial copy as a full transfer, which matters because the
        // engine accumulates request.length for completed tasks. Compared with
        // '<' rather than '!=': the reported count is a posted-byte count, so
        // it must never be trusted to be short, but a larger value is not an
        // error.
        LOG(WARNING) << "MpcommTransport: Short transfer, "
                     << outcome.bytes_transferred << " of "
                     << task.request.length << " bytes";
        task.transferred_bytes = outcome.bytes_transferred;
        task.status_word = TransferStatusEnum::FAILED;
    } else {
        task.status_word = TransferStatusEnum::COMPLETED;
        task.transferred_bytes = outcome.bytes_transferred;
    }
    // Released on every terminal outcome, and the handle is cleared so that a
    // repeated poll of the same task cannot release it a second time.
    adapter.releaseTransfer(task.mpcomm_handle);
    task.mpcomm_handle = kInvalidMpcommTransferHandle;
}

}  // namespace tent
}  // namespace mooncake
