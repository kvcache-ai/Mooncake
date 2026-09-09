// Copyright 2025 KVCache.AI
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

#ifndef TENT_SLICE_H
#define TENT_SLICE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "tent/runtime/transport.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {
struct RdmaSlice;
class RailMonitor;

struct RdmaSliceList {
    RdmaSlice* first = nullptr;
    int num_slices = 0;
};

// `count` slices: the first count - 1 of `block_size` bytes and the last
// holding what remains, which is a short tail folded in when one was worth
// folding -- so up to `block_size` more than a block, never less than one
// byte.
struct RdmaSlicePlan {
    uint64_t block_size = 0;
    uint64_t count = 0;
};

// Cut `length` into at most `max_slices` slices, each a whole number of
// `base_block` bytes. Rounding the block up covers the request in fewer
// slices than were asked for, so the count comes from the block and not the
// other way round: an empty slice would still cost a work request, a CQE, a
// path selection and a completion.
//
// A last slice shorter than `merge_ratio` of a block is given to the slice
// before it instead of being posted on its own, trading a fraction of a
// block of extra work on that one slice for a work request, a CQE and a
// completion. This has to happen after the block is chosen: asking for one
// slice fewer up front only makes the block round up to the next whole one,
// which leaves the short tail as its own slice anyway and spreads the
// request over half as many slices. A ratio of 0 never folds.
//
// Zero length keeps one slice -- task accounting counts slices, and a task
// with none never reaches a terminal status.
inline RdmaSlicePlan planRdmaSlices(uint64_t length, uint64_t base_block,
                                    uint64_t max_slices,
                                    double merge_ratio = 0.25) {
    if (base_block == 0) base_block = 1;
    if (max_slices == 0) max_slices = 1;
    if (length == 0) return {base_block, 1};

    uint64_t count = (length + base_block - 1) / base_block;
    if (count > max_slices) count = max_slices;

    const uint64_t per_slice = (length + count - 1) / count;
    const uint64_t block_size = (per_slice % base_block == 0)
                                    ? per_slice
                                    : (per_slice / base_block + 1) * base_block;

    count = (length + block_size - 1) / block_size;
    if (count > 1) {
        const uint64_t tail = length - (count - 1) * block_size;
        if (static_cast<double>(tail) < merge_ratio * block_size) --count;
    }
    return {block_size, count};
}

// Forward declarations
class RdmaEndPoint;
struct RdmaTask;

using RdmaSliceStorage = Slab<RdmaSlice>;
using RdmaTaskStorage = Slab<RdmaTask>;

struct RdmaTask {
    int num_slices;
    Request request;
    // Resolved by TransportSelector and copied from RdmaSubBatch. The
    // per-slice path runs later on worker threads, so it must carry the same
    // device policy as the aggregate allocation path.
    uint64_t device_mask{~0ULL};
    // Named QP pool this task's slices should use (RFC #2568 step 3). Empty =
    // no pool selected: slices spray across all data QPs as before. Resolved
    // from SelectionResult.qp_pool at task creation.
    std::string qp_pool;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    std::atomic<int> success_slices{0};
    std::atomic<int> resolved_slices{0};
    volatile TransferStatusEnum first_error = PENDING;

    // Set by the control thread. Workers observe this flag before posting or
    // retrying a slice. Already-posted WRs are allowed to drain normally.
    std::atomic<bool> cancel_requested{false};

    // Reference counting for UAF protection
    std::atomic<int> ref_count{0};

    void ref() { ref_count.fetch_add(1, std::memory_order_relaxed); }
    void deref() {
        if (ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            RdmaTaskStorage::Get().deallocate(this);
        }
    }
};

class RdmaEndPoint;

struct RdmaSlice {
    void* source_addr = nullptr;
    uint64_t target_addr = 0;
    size_t length = 0;

    RdmaTask* task = nullptr;
    RdmaSlice* next = nullptr;

    uint32_t source_lkey = 0;
    uint32_t target_rkey = 0;
    int source_dev_id = -1;
    int target_dev_id = -1;
    // GPUDirect reachability learning (see GdrReachability). Resolved once per
    // (re)submit in Workers::generatePostPath. GPU ordinals are -1 for host
    // memory; the name pointers alias stable Topology::NicEntry / segment
    // storage and stay valid for the slice's lifetime.
    int source_gpu_ordinal = -1;
    int target_gpu_ordinal = -1;
    const char* source_nic_name = nullptr;
    const char* target_nic_name = nullptr;
    const std::string* target_machine_id = nullptr;

    std::weak_ptr<RdmaEndPoint> ep_weak_ptr;
    TransferStatusEnum word = TransferStatusEnum::INITIAL;
    int qp_index = 0;
    // Worker lane that enqueued this slice: the one whose inflight_slice_set
    // holds it. Whichever lane later sweeps the slice off a queue pair must
    // hand the set entry back to this one -- with qp_pools several lanes can
    // share a queue pair, so the sweeper is often somebody else. -1 until
    // Workers::submit() picks a lane. Atomic because a retry re-points it to
    // the lane that re-queued the slice (Workers::submitFromTick) while the
    // lane it is leaving may still hold a set entry it has not drained, and
    // reads that entry to route the removal home.
    std::atomic<int> owner_worker{-1};
    // Worker lane whose inflight_slices counts this slice, -1 while none
    // does. Each accounting the slice carries names the counter it sits in,
    // so whichever path takes it out -- its own completion, another lane's
    // sweep of the queue pair they share, the timeout pass that finds it
    // already terminal -- exchanges the field for -1 and pays back exactly
    // what it read; a second path reads -1 and does nothing. Usually the
    // owner lane, but a retry moves the count in one exchange
    // (Workers::submitFromTick) while the old set entry drains later.
    std::atomic<int> counted_lane{-1};
    int retry_count = 0;
    // Flat (source,target) combination index last tried by
    // selectFallbackDevice; the next fallback resumes just past it so retries
    // rotate through all combinations with wraparound instead of hammering one.
    int last_fallback_idx = -1;
    bool failed = false;
    // Device DeviceSelector accounts this slice against, -1 while none.
    // Same discipline as counted_lane: whoever releases the charge --
    // completion, failure, timeout or cancel -- exchanges it for -1 and
    // pays that device, so a fallback that has already rewritten
    // source_dev_id cannot misdirect the release. Atomic because on a pooled
    // QP the timeout sweep and the CQ poller run on different workers.
    std::atomic<int> charged_dev{-1};
    // Device whose posted backlog counts this slice: set when the work
    // request reaches the hardware, -1 again once the slice leaves the queue
    // pair. A charged slice waiting in a worker queue is not posted.
    std::atomic<int> posted_dev{-1};
    uint64_t enqueue_ts = 0;
    uint64_t submit_ts = 0;
    // Non-owning pointer to the per-worker RailMonitor for this slice's
    // target machine, resolved once in generatePostPath. Lets asyncPollCq
    // and disableEndpoint call markRecovered / markFailed without a
    // string-keyed map lookup on the RDMA hot path. Stable because
    // WorkerContext::rails stores values via unique_ptr, so rehashes do
    // not invalidate the pointee.
    RailMonitor* rail_monitor = nullptr;
    int priority = PRIO_HIGH;
};

static inline void updateSliceStatus(RdmaSlice* slice,
                                     TransferStatusEnum status) {
    if (status == PENDING) return;
    RdmaTask* task = slice->task;
    if (!__sync_bool_compare_and_swap(&slice->word, PENDING, status)) return;
    if (status == COMPLETED) {
        __sync_fetch_and_add(&task->transferred_bytes, slice->length);
        task->success_slices.fetch_add(1, std::memory_order_acq_rel);
    } else {
        __sync_bool_compare_and_swap(&task->first_error, PENDING, status);
    }
    int resolved =
        task->resolved_slices.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (resolved >= task->num_slices) {
        TransferStatusEnum final_st =
            (task->success_slices.load(std::memory_order_acquire) ==
             task->num_slices)
                ? COMPLETED
                : task->first_error;
        if (final_st == PENDING) final_st = FAILED;
        __sync_bool_compare_and_swap(&task->status_word, PENDING, final_st);
    }
    task->deref();
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SLICE_H
