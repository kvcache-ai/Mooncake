// Copyright 2024 KVCache.AI
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

#include "transport/rdma_transport/rdma_transport.h"
#include "transport/transport.h"
#include "transport/rdma_transport/pxn/pxn_types.h"
#include "transport/rdma_transport/pxn/pxn_object_cache.h"

namespace mooncake {

// RdmaTransport-specific PXN slice completion handler
// This is called automatically when a slice completes (markSuccess/markFailed)
void RdmaTransport::pxnSliceCompletion(Slice *slice) {
    if (!slice) return;
    if (slice->pxn_info.is_copy_worker) return;

    // Get the shell slice (which contains the CopyWorker)
    auto *shell = reinterpret_cast<Transport::Slice *>(slice->pxn_info.pxn_copy_worker);
    if (shell == nullptr) return;

    auto *cw = reinterpret_cast<pxn::PxnCopyWorker *>(shell->pxn_info.pxn_copy_worker);
    if (cw == nullptr) return;

    // Clear the pointer to prevent duplicate calls
    slice->pxn_info.pxn_copy_worker = nullptr;

    // Last worker: handle cascade / slot release (but do NOT free the CW).
    auto *pool = reinterpret_cast<pxn::PxnSlotPool *>(cw->pxn_slot_pool);
    if (pool == nullptr) return;

    // Decrement reference count atomically
    int prev = cw->pxn_refcount.fetch_sub(1, std::memory_order_acq_rel);
    if (prev != 1) return;  // Not the last worker, return early

    void *next_shell = cw->pxn_cascade_next;
    if (next_shell != nullptr) {
        // Signal next copy worker in cascade to proceed
        auto *next_slice = static_cast<Transport::Slice *>(next_shell);
        auto *next_cw = static_cast<pxn::PxnCopyWorker *>(
            next_slice->pxn_info.pxn_copy_worker);
        if (next_cw != nullptr) {
            next_cw->pxn_state.store(pxn::PxnCopyWorker::kReadyToCopy,
                                     std::memory_order_release);
        }
    } else {
        // Mark slot as available for reuse
        auto &slot = pool->pxn_slots[cw->pxn_slot_idx];
        slot.pxn_used.store(0, std::memory_order_release);
    }
}

// Transport-vtable override: reclaim CWs held by a completed TransferTask.
// Called from ~TransferTask via Transport::pxnFreeTaskCws(). Non-PXN
// transports keep the empty base implementation and never link this code.
void RdmaTransport::pxnFreeTaskCws(std::vector<void *> &cws) {
    for (auto *cw_raw : cws) {
        auto *cw = static_cast<pxn::PxnCopyWorker *>(cw_raw);
        if (cw == nullptr) continue;
        int prev = cw->pxn_task_refcount.fetch_sub(
            1, std::memory_order_acq_rel);
        if (prev == 1) {
            // Last task holding this CW: safe to return to the pool.
            pxn::PxnGetCopyWorkerCache().PxnDeallocate(cw);
        }
    }
    cws.clear();
}

}  // namespace mooncake
