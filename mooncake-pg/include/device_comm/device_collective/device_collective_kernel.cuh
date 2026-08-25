#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH

#include <cooperative_groups.h>
#include <cuda/atomic>
#include <transport/device/device_ops.cuh>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_collective/device_control_update.cuh"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"

namespace mooncake {

struct CollectivePreparationResult {
    InGroupRank failed_rank = kInvalidInGroupRank;

    [[nodiscard]] __device__ __forceinline__ bool succeeded() const {
        return failed_rank == kInvalidInGroupRank;
    }
};

// A protocol describes only the peers whose control state must match before
// communication starts. The common startup path owns the synchronization
// procedure itself.
struct ViewEpochPeer {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    uint64_t signal_offset = 0;
};

// Publish this invocation's View epoch to every required peer before waiting
// for any peer. This ordering prevents peers from forming a startup cycle.
[[nodiscard]] __device__ __forceinline__ CollectivePreparationResult
synchronizeCollectiveViewEpoch(uint64_t view_epoch, uint64_t timeout_ticks,
                               const uint64_t* view_epoch_signals,
                               const ViewEpochPeer* peers, uint32_t peer_count,
                               const TransferLane& lane,
                               cooperative_groups::thread_block block) {
    PG_DEVICE_ASSERT(view_epoch != kInvalidViewEpoch);
    PG_DEVICE_ASSERT(view_epoch_signals);
    PG_DEVICE_ASSERT(peers || peer_count == 0);

    // Phase 1 publishes to every peer before any wait begins.
    InGroupRank failed_rank = kInvalidInGroupRank;
    for (uint32_t index = 0; index < peer_count; ++index) {
        const auto& peer = peers[index];
        PG_DEVICE_ASSERT(peer.global_rank != kInvalidGlobalRank);
        PG_DEVICE_ASSERT(peer.in_group_rank != kInvalidInGroupRank);

        SignalRequest request;
        request.signal.kind = SignalAction::Kind::Set;
        request.signal.remote_offset = peer.signal_offset;
        request.signal.set.value = view_epoch;
        request.timeout_ticks = timeout_ticks;
        if (lane.signal(peer.global_rank, request, block).wait(block) !=
                TransferResult::Succeeded &&
            failed_rank == kInvalidInGroupRank) {
            failed_rank = peer.in_group_rank;
        }
    }
    if (failed_rank != kInvalidInGroupRank) {
        return {.failed_rank = failed_rank};
    }

    // Phase 2 waits on the local signal owned by each peer.
    for (uint32_t index = 0; index < peer_count; ++index) {
        const auto& peer = peers[index];
        SignalWaitRequest request;
        request.local_ptr = view_epoch_signals + peer.in_group_rank;
        request.least = view_epoch;
        request.timeout_ticks = timeout_ticks;
        const auto ready = lane.waitSignal(request, block);
        if (ready.status != SignalWaitStatus::Reached ||
            ready.observed != view_epoch) {
            return {.failed_rank = peer.in_group_rank};
        }
    }
    return {};
}

// Elect the first resident CTA to apply one pending control update, then check
// the updated Plan's required view-epoch peers. Every other CTA stays outside
// protocol state until preparation succeeds or reports one failed peer.
//
// The first CTA to reach this function becomes the startup leader. CUDA does
// not guarantee that block 0 becomes resident first; if resident CTAs waited
// for an unscheduled block 0, they could occupy every available CTA slot and
// prevent block 0 from ever running.
//
// InvocationState is reused across kernel launches. StrongStream guarantees
// that only one collective invocation uses it at a time.
template <typename Plan, typename PrepareProtocol>
[[nodiscard]] __device__ __forceinline__ CollectivePreparationResult
prepareCollectiveInvocation(PlanSlot<Plan>* plan_slot,
                            const uint64_t* view_epoch_signals,
                            InvocationState* invocation,
                            ControlMailbox* control_mailbox,
                            uint32_t lane_index,
                            PrepareProtocol prepare_protocol,
                            cooperative_groups::thread_block block) {
    __shared__ uint32_t is_startup_leader;
    if (block.thread_rank() == 0) {
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
            startup_arrival_count(invocation->startup_arrival_count);
        const uint32_t previous_arrival_count =
            startup_arrival_count.fetch_add(1, cuda::memory_order_relaxed);
        is_startup_leader = previous_arrival_count == 0;
    }
    block.sync();

    if (is_startup_leader != 0) {
        if (block.thread_rank() == 0) {
            applyPendingControlUpdate(&control_mailbox->control_update_slot);
        }
        block.sync();

        PG_DEVICE_ASSERT(plan_slot->status == DevicePlanStatus::Ready);
        const auto preparation = prepare_protocol(
            plan_slot->plan, view_epoch_signals, lane_index, block);
        block.sync();
        if (block.thread_rank() == 0) {
            invocation->failed_rank = preparation.failed_rank;
            cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
                startup_complete(invocation->startup_complete);
            // Publish failed_rank to the acquire loads in the other CTAs.
            startup_complete.store(1, cuda::memory_order_release);
        }
    } else if (block.thread_rank() == 0) {
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device> startup_complete(
            invocation->startup_complete);
        while (startup_complete.load(cuda::memory_order_acquire) == 0) {
        }
    }

    block.sync();
    return {.failed_rank = invocation->failed_rank};
}

// Completes this channel after success or a locally detected failure. Only a
// detecting channel supplies a failed rank; the first detector records the
// failure metadata.
__device__ __forceinline__ void completeChannel(
    InvocationState* invocation, ControlMailbox* control_mailbox,
    cooperative_groups::thread_block block,
    InGroupRank detected_failed_rank = kInvalidInGroupRank,
    int32_t* failed_ranks_hint = nullptr) {
    // No thread may publish channel completion while another thread in the CTA
    // can still access the current Plan or protocol buffers.
    block.sync();
    if (block.thread_rank() == 0) {
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device> failure_latched(
            invocation->failure_latched);
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
            completion_arrival_count(invocation->completion_arrival_count);
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device> startup_complete(
            invocation->startup_complete);
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
            startup_arrival_count(invocation->startup_arrival_count);

        // This CAS only elects the metadata writer. The completion-arrival
        // RMW below publishes the metadata written after a successful CAS.
        uint32_t expected_failure = 0;
        if (detected_failed_rank != kInvalidInGroupRank &&
            failure_latched.compare_exchange_strong(
                expected_failure, 1, cuda::memory_order_relaxed,
                cuda::memory_order_relaxed)) {
            invocation->failed_rank = detected_failed_rank;
            invocation->failed_hint_address =
                reinterpret_cast<uint64_t>(failed_ranks_hint);
        }

        // Each acq_rel increment publishes this CTA's prior accesses and
        // carries visibility from earlier arrivals. The final arriving CTA
        // therefore observes every channel quiescent before replacing Plan or
        // protocol state.
        const uint32_t previous_arrival_count =
            completion_arrival_count.fetch_add(1, cuda::memory_order_acq_rel);
        if (previous_arrival_count + 1 == gridDim.x) {
            if (failure_latched.load(cuda::memory_order_relaxed) != 0) {
                const uint64_t generation =
                    device::mc_ld_acquire_u64(
                        &control_mailbox->failure_generation) +
                    1;

                control_mailbox->failed_rank = invocation->failed_rank;
                control_mailbox->failed_hint_address =
                    invocation->failed_hint_address;

                // Make the copied failure metadata system-visible before the
                // release store notifies the host. The matching acquire load
                // lets the host read the metadata and replace the Plan and
                // protocol state only after all channels are quiescent.
                __threadfence_system();
                device::mc_st_release_u64(&control_mailbox->failure_generation,
                                          generation);
                while (device::mc_ld_acquire_u64(
                           &control_mailbox->ready_generation) < generation) {
                }
                applyPinnedControlUpdate(&control_mailbox->control_update_slot);
            }

            // Every CTA has finished using these fields, and StrongStream
            // prevents the next invocation from reusing them until this kernel
            // exits. No release ordering is needed for these reset stores.
            failure_latched.store(0, cuda::memory_order_relaxed);
            completion_arrival_count.store(0, cuda::memory_order_relaxed);
            startup_complete.store(0, cuda::memory_order_relaxed);
            startup_arrival_count.store(0, cuda::memory_order_relaxed);
        }
    }
    block.sync();
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
