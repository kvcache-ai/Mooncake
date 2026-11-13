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

#ifndef MOONCAKE_PXN_TYPES_H_
#define MOONCAKE_PXN_TYPES_H_

#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {
namespace pxn {

// Default slot_size = 1 MiB
constexpr size_t kPxnDefaultSlotSize = 1 * 1024 * 1024;
// Default slot count per pool
constexpr int kPxnDefaultSlotCount = 16;

// hang on copy worker
struct PxnSegRange {
    uint64_t pxn_src_start = 0;
    uint64_t pxn_src_end = 0;
    size_t pxn_offset_in_slot = 0;
    bool pxn_from_cache = false;
};

struct PxnCopyWorker {
    //   kPending
    //     └─ PxnSubmitPendingWorkers assigns a slot to the cw:
    //        ├─ slot free  (CAS success) → kMemcpyIssued (added to batch)
    //        ├─ slot busy, cascade_tail non-null → kWaitingCascade
    //        └─ slot busy, cascade_tail null     → kWaitingSlot
    //
    //   kWaitingSlot
    //     └─ next PxnSubmitPendingWorkers pass retries the same slot:
    //        ├─ CAS success → kMemcpyIssued
    //        └─ still busy  → stay in kWaitingSlot
    //
    //   kWaitingCascade
    //     └─ pollCq (PxnHandleCqe) triggers the memcpy for us and moves
    //        us to kMemcpyIssued
    //
    //   kMemcpyIssued
    //     └─ send thread (PxnPromoteDoneWorkers) sees the event completed
    //        and transitions us to kMemcpyDone, then hands the packed
    //        Slices off to collective_slice_queue.
    enum State : uint8_t {
        kPending = 0,
        kWaitingSlot = 1,
        kWaitingCascade = 2,
        kReadyToCopy = 3,
        kMemcpyIssued = 4,
        kMemcpyDone = 5,
    };

    // ---- Owner / routing ----
    // Worker thread ID + slot index + slot pool pointer
    int pxn_worker_thread_id = -1;
    int pxn_slot_idx = -1;
    void* pxn_slot_pool = nullptr;

    // ---- Packed slices ----
    // pack all original slice pointers into this slot + the
    // offset of each slice within the slot.
    std::vector<void*> pxn_rdma_workers;
    std::vector<size_t> pxn_offsets_in_slot;
    size_t pxn_cumulative_size = 0;
    size_t pxn_rdma_workers_cnt = 0;

    // ---- Seg-level memcpy ranges ----
    // A contiguous source memory segment at the seg granularity
    // (multiple slices within the same seg are merged into one).
    std::vector<PxnSegRange*> pxn_seg_ranges;

    // ---- State machine ----
    std::atomic<uint8_t> pxn_state{kPending};
    std::atomic<int> pxn_refcount{0};
    // Number of TransferTasks that hold this CW in their pxn_copy_worker_list.
    // Incremented at ~PxnEnsureSlotSpace when the CW is (re)associated with a
    // task, decremented in PxnFreeTaskCws; only the last dropper actually
    // returns the CW to the TLS pool via PxnDeallocate.
    std::atomic<int> pxn_task_refcount{0};

    // ---- CUDA event association ----
    // Points into slot_pool->slot_events[]. Multiple copy workers may share
    // one event under cudaEventRecord aggregation (v2 spec 4.2.4 scenario 1).
    cudaEvent_t* pxn_event_ptr = nullptr;

    // ---- Cascade chain (v2 spec 4.2.2) ----
    // point cascade next copy worker(Slice)
    void* pxn_cascade_next = nullptr;

    // ---- Source routing (used at scheduling / debugging) ----
    int pxn_src_gpu_id = -1;    // cuda device id
    int pxn_proxy_gpu_id = -1;  // cuda device id

    // ---- Cache bookkeeping ----
    // Whether it comes from the object pool (true = reuse;
    // false = new allocation). Hot paths are not currently used.
    bool pxn_from_cache = false;
    int peer_device_id;
};

struct alignas(64) PxnSlotEntry {
    // 0 = free, 1 = occupied. Set to 1 via CAS in submitPostSend when a
    // worker claims the slot; set back to 0 by the CQE handler after all
    // packed slices have completed on the wire.
    std::atomic<uint8_t> pxn_used{0};

    // recorded by send/cascade stream
    cudaEvent_t pxn_send_event{nullptr};

    // save slot cascade copy worker list
    void* pxn_cascade_head{nullptr};
    void* pxn_cascade_tail{nullptr};

    bool pxn_in_pending_batch{false};
};

struct PxnSlotPool {
    // Configurable at construction.
    size_t pxn_slot_size = kPxnDefaultSlotSize;
    int pxn_slot_count = kPxnDefaultSlotCount;

    // save worker_idx, each worker pool thread corresponds to one other thread.
    int pxn_pool_index = -1;

    // cuda device id on which dev_pool lives
    int pxn_proxy_gpu_id = -1;
    void* pxn_dev_pool = nullptr;
    ibv_mr* pxn_mr = nullptr;

    // each slot state
    std::vector<PxnSlotEntry> pxn_slots;

    // the copy worker selects slots in sequence.
    uint32_t pxn_global_step = 0;

    // cascade not empty slot count
    int pxn_active_slot_count = 0;

    PxnSlotPool() = default;
    PxnSlotPool(const PxnSlotPool&) = delete;
    PxnSlotPool& operator=(const PxnSlotPool&) = delete;
};

struct PxnCascadeBatch {
    // Size fixed to pxn_slot_pools.size() at init time; index i holds
    // the pending cascade cw for the pool at pxn_slot_pools[i].
    std::vector<std::vector<void*>> pxn_buckets;
    bool havedata = false;
};

}  // namespace pxn
}  // namespace mooncake

#endif  // MOONCAKE_PXN_TYPES_H_
