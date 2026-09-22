#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH

#include <type_traits>

#include <cooperative_groups.h>
#include <cuda/atomic>

#include "common_types.h"
#include "device_comm/device_utils/d2h_request_slot.cuh"
#include "device_comm/device_utils/device_assert.cuh"
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

// Publish this invocation's View epoch to every required peer before waiting
// for any peer. This ordering prevents peers from forming a startup cycle.
// Algorithms select group ranks; the protocol's peer table supplies the DTS
// rank and remote View-epoch word resolved by the runtime.
template <typename Peer>
[[nodiscard]] __device__ __forceinline__ CollectivePreparationResult
synchronizeCollectiveViewEpoch(uint64_t view_epoch, uint64_t timeout_ticks,
                               const uint64_t* view_epoch_signals,
                               const Peer* peer_bindings,
                               const InGroupRank* peers, uint32_t peer_count,
                               const TransferLane& lane,
                               cooperative_groups::thread_block block) {
    PG_DEVICE_ASSERT(view_epoch != kInvalidViewEpoch);
    PG_DEVICE_ASSERT(view_epoch_signals);
    PG_DEVICE_ASSERT(peers || peer_count == 0);

    // Phase 1 publishes to every peer before any wait begins.
    InGroupRank failed_rank = kInvalidInGroupRank;
    for (uint32_t index = 0; index < peer_count; ++index) {
        const auto rank = peers[index];
        PG_DEVICE_ASSERT(rank >= 0 &&
                         static_cast<uint32_t>(rank) < kMaxNumRanks);
        const auto& peer = peer_bindings[rank];
        PG_DEVICE_ASSERT(peer.global_rank != kInvalidGlobalRank);

        SignalRequest request;
        request.signal.kind = SignalAction::Kind::Set;
        request.signal.remote_offset = peer.view_epoch_signal_offset;
        request.signal.set.value = view_epoch;
        request.timeout_ticks = timeout_ticks;
        if (lane.signal(peer.global_rank, request, block).wait(block) !=
                TransferResult::Succeeded &&
            failed_rank == kInvalidInGroupRank) {
            failed_rank = rank;
        }
    }
    if (failed_rank != kInvalidInGroupRank) {
        return {.failed_rank = failed_rank};
    }

    // Phase 2 waits on the local signal owned by each peer.
    for (uint32_t index = 0; index < peer_count; ++index) {
        const auto rank = peers[index];
        SignalWaitRequest request;
        request.local_ptr = view_epoch_signals + rank;
        request.least = view_epoch;
        request.timeout_ticks = timeout_ticks;
        const auto ready = lane.waitSignal(request, block);
        if (ready.status != SignalWaitStatus::Reached ||
            ready.observed != view_epoch) {
            return {.failed_rank = rank};
        }
    }
    return {};
}

template <typename Peer, uint32_t PeerCapacity>
__device__ __forceinline__ void drainCollectiveTransfers(
    const DeviceTransferHandle& handle, const Peer* peer_bindings,
    const InGroupRank (&peers)[PeerCapacity], uint32_t peer_count) {
    PG_DEVICE_ASSERT(peer_count <= PeerCapacity);
    GlobalRank transfer_peers[PeerCapacity];
    for (uint32_t index = 0; index < peer_count; ++index) {
        const auto rank = peers[index];
        PG_DEVICE_ASSERT(rank >= 0 &&
                         static_cast<uint32_t>(rank) < kMaxNumRanks);
        transfer_peers[index] = peer_bindings[rank].global_rank;
    }
    drainTransfers(handle, transfer_peers, peer_count);
}

// A single CTA applies the control update and prepares the algorithm directly.
// With multiple CTAs, elect the first resident CTA to apply one pending control
// update, then check the updated Plan's required view-epoch peers. Every other
// CTA stays outside algorithm state until preparation completes.
//
// The first CTA to reach this function becomes the startup leader. CUDA does
// not guarantee that block 0 becomes resident first; if resident CTAs waited
// for an unscheduled block 0, they could occupy every available CTA slot and
// prevent block 0 from ever running.
//
// InvocationState is reused across kernel launches. StrongStream guarantees
// that only one collective invocation uses it at a time.
template <typename Scope = MultiCta, typename Plan, typename PrepareAlgorithm>
[[nodiscard]] __device__ __forceinline__ CollectivePreparationResult
prepareCollectiveInvocation(PlanSlot<Plan>* plan_slot,
                            const uint64_t* view_epoch_signals,
                            InvocationState* invocation,
                            ControlMailbox* control_mailbox,
                            uint32_t lane_index,
                            PrepareAlgorithm prepare_algorithm,
                            cooperative_groups::thread_block block) {
    static_assert(std::is_same_v<Scope, SingleCta> ||
                  std::is_same_v<Scope, MultiCta>);

    if constexpr (std::is_same_v<Scope, SingleCta>) {
        if (block.thread_rank() == 0) {
            applyPendingControlUpdate(&control_mailbox->control_update_slot);
        }
        block.sync();

        PG_DEVICE_ASSERT(plan_slot->status == DevicePlanStatus::Ready);
        const auto preparation = prepare_algorithm(
            plan_slot->plan, view_epoch_signals, lane_index, block);
        block.sync();
        return preparation;
    } else {
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
                applyPendingControlUpdate(
                    &control_mailbox->control_update_slot);
            }
            block.sync();

            PG_DEVICE_ASSERT(plan_slot->status == DevicePlanStatus::Ready);
            const auto preparation = prepare_algorithm(
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
            cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
                startup_complete(invocation->startup_complete);
            while (startup_complete.load(cuda::memory_order_acquire) == 0) {
            }
        }

        block.sync();
        return {.failed_rank = invocation->failed_rank};
    }
}

// Completes this channel after success or a locally detected failure. Only a
// detecting channel supplies a failed rank; the first detector records the
// failure metadata.
template <typename DrainTransfers>
__device__ __noinline__ void completeChannel(
    InvocationState* invocation, ControlMailbox* control_mailbox,
    DrainTransfers drain_transfers, cooperative_groups::thread_block block,
    InGroupRank detected_failed_rank = kInvalidInGroupRank,
    int32_t* failed_ranks_hint = nullptr) {
    // No thread may publish channel completion while another thread in the CTA
    // can still access the current Plan or algorithm buffers.
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
        // algorithm state.
        const uint32_t previous_arrival_count =
            completion_arrival_count.fetch_add(1, cuda::memory_order_acq_rel);
        if (previous_arrival_count + 1 == gridDim.x) {
            if (failure_latched.load(cuda::memory_order_relaxed) != 0) {
                // A failed channel may leave payload transfers outstanding.
                // Drain before recovery can replace algorithm state.
                drain_transfers();

                control_mailbox->recovery
                    .submit(CollectiveFailureReport{
                        .failed_rank = invocation->failed_rank,
                        .failed_hint_address = invocation->failed_hint_address,
                    })
                    .wait();
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
