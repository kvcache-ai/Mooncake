#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH

#include <cooperative_groups.h>
#include <cuda/atomic>

#include "common_types.h"
#include "device_comm/device_utils/d2h_request_slot.cuh"
#include "pg_assert.h"
#include "device_comm/device_collective/device_control_update.cuh"
#include "device_comm/device_collective/device_collective_local.cuh"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"

namespace mooncake {

enum class CollectiveExecution { SingleCTA, MultiCTA };

namespace detail {

__device__ __forceinline__ void drainCollectiveTransfers(
    const DeviceTransferHandle& handle, const DeviceGroupView& view) {
    PG_ASSERT(view.participant_count <= kMaxNumRanks);
    GlobalRank transfer_peers[kMaxNumRanks];
    uint32_t peer_count = 0;
    for (uint32_t index = 0; index < view.participant_count; ++index) {
        if (index == static_cast<uint32_t>(view.self_active_index)) continue;
        transfer_peers[peer_count++] = view.participants[index].global_rank;
    }
    if (peer_count != 0) handle.drain(transfer_peers, peer_count);
}

// Publish to all required peers before waiting, avoiding a startup cycle.
[[nodiscard]] __device__ __forceinline__ CollectiveResult
synchronizeCollectiveViewEpoch(const DeviceGroupView& view,
                               uint64_t timeout_ticks,
                               const uint64_t* view_epoch_signals,
                               const TransferLane& lane,
                               cooperative_groups::thread_block block) {
    PG_ASSERT(view.epoch != kInvalidViewEpoch);
    PG_ASSERT(view_epoch_signals);

    // Phase 1 publishes to every peer before any wait begins.
    InGroupRank failed_rank = kInvalidInGroupRank;
    for (uint32_t index = 0; index < view.participant_count; ++index) {
        if (index == static_cast<uint32_t>(view.self_active_index)) continue;
        const auto& peer = view.participants[index];
        const auto rank = peer.in_group_rank;
        PG_ASSERT(rank >= 0 && static_cast<uint32_t>(rank) < kMaxNumRanks);
        PG_ASSERT(peer.global_rank != kInvalidGlobalRank);

        SignalRequest request;
        request.signal.kind = SignalAction::Kind::Set;
        request.signal.remote_offset = peer.view_epoch_signal_offset;
        request.signal.set.value = view.epoch;
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
    for (uint32_t index = 0; index < view.participant_count; ++index) {
        if (index == static_cast<uint32_t>(view.self_active_index)) continue;
        const auto& peer = view.participants[index];
        const auto rank = peer.in_group_rank;
        SignalWaitRequest request;
        request.local_ptr = view_epoch_signals + rank;
        request.least = view.epoch;
        request.timeout_ticks = timeout_ticks;
        const auto ready = lane.waitSignal(request, block);
        if (ready.status != SignalWaitStatus::Reached ||
            ready.observed != view.epoch) {
            return {.failed_rank = rank};
        }
    }
    return {};
}

// Apply control updates before the startup CTA reads algorithm state.
template <typename Plan>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
prepareCollectiveInvocation(Plan* plan,
                            const CollectiveRuntimeBindings& collective,
                            cooperative_groups::thread_block block) {
    PG_ASSERT(plan && collective.control_mailbox);
    if (block.thread_rank() == 0) {
        collective.transfer_handle->pauseIfRequested();
    }
    applyPendingControlUpdate(&collective.control_mailbox->control_update_slot,
                              block);
    PG_ASSERT(plan->status == DevicePlanStatus::Ready);
    auto& view = *collective.group_view;
    // Single-participant execution needs no communication preparation.
    if (view.participant_count == 1 || view.synchronized) return {};

    PG_COLL_TRY(synchronizeCollectiveViewEpoch(
        view, collective.timeout_ticks, collective.view_epoch_signals,
        collective.transfer_handle->lane(0), block));
    if (block.thread_rank() == 0) view.synchronized = true;
    block.sync();
    return {};
}

template <typename Plan>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
beginMultiCTAInvocation(Plan* plan, const CollectiveRuntimeBindings& collective,
                        cooperative_groups::thread_block block) {
    auto* const invocation = collective.invocation_state;
    PG_ASSERT(invocation);
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
        const auto preparation =
            prepareCollectiveInvocation(plan, collective, block);
        block.sync();
        if (block.thread_rank() == 0) {
            invocation->result = preparation;
            cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
                startup_complete(invocation->startup_complete);
            // Publish the result to the acquire loads in the other CTAs.
            startup_complete.store(1, cuda::memory_order_release);
        }
    } else if (block.thread_rank() == 0) {
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device> startup_complete(
            invocation->startup_complete);
        while (startup_complete.load(cuda::memory_order_acquire) == 0) {
        }
    }

    block.sync();
    return invocation->result;
}

// One CTA recovers after every CTA has stopped using the old Plan.
static __device__ __noinline__ void recoverCollectiveFailure(
    const CollectiveRuntimeBindings& collective, CollectiveResult failure,
    uint64_t failed_hint_address, cooperative_groups::thread_block block) {
    if (block.thread_rank() == 0) {
        drainCollectiveTransfers(*collective.transfer_handle,
                                 *collective.group_view);
        auto recovery =
            collective.control_mailbox->recovery.submit(CollectiveFailureReport{
                .result = failure,
                .failed_hint_address = failed_hint_address,
            });
        // This CTA no longer uses DTS while waiting for recovery. Acknowledge
        // pause requests here so DTS can switch route resources before the
        // kernel exits.
        while (!recovery.poll()) collective.transfer_handle->pauseIfRequested();
    }
    applyPinnedControlUpdate(&collective.control_mailbox->control_update_slot,
                             block);
}

// Every CTA arrives once; the last arrival owns recovery and state reset.
static __device__ __noinline__ void finishMultiCTAInvocation(
    const CollectiveRuntimeBindings& collective,
    cooperative_groups::thread_block block, CollectiveResult result = {},
    int32_t* failed_ranks_hint = nullptr) {
    auto* const invocation = collective.invocation_state;
    cuda::atomic_ref<uint32_t, cuda::thread_scope_device> failure_latched(
        invocation->failure_latched);
    cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
        completion_arrival_count(invocation->completion_arrival_count);
    __shared__ uint32_t is_last_arrival;
    __shared__ uint32_t needs_recovery;
    // Finish all accesses to the current Plan before publishing completion.
    block.sync();
    if (block.thread_rank() == 0) {
        // This CAS only elects the metadata writer. The completion-arrival
        // RMW below publishes the metadata written after a successful CAS.
        uint32_t expected_failure = 0;
        if (!result.succeeded() &&
            failure_latched.compare_exchange_strong(
                expected_failure, 1, cuda::memory_order_relaxed,
                cuda::memory_order_relaxed)) {
            invocation->result = result;
            invocation->failed_hint_address =
                reinterpret_cast<uint64_t>(failed_ranks_hint);
        }

        // Each acq_rel increment publishes this CTA's prior accesses and
        // carries visibility from earlier arrivals. The final arriving CTA
        // therefore observes every channel quiescent before replacing Plan or
        // algorithm state.
        const uint32_t previous_arrival_count =
            completion_arrival_count.fetch_add(1, cuda::memory_order_acq_rel);
        is_last_arrival = previous_arrival_count + 1 == gridDim.x;
        needs_recovery = is_last_arrival &&
                         failure_latched.load(cuda::memory_order_relaxed) != 0;
    }
    block.sync();
    if (is_last_arrival == 0) return;

    if (needs_recovery != 0) {
        recoverCollectiveFailure(collective, invocation->result,
                                 invocation->failed_hint_address, block);
    }
    if (block.thread_rank() == 0) {
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device> startup_complete(
            invocation->startup_complete);
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device>
            startup_arrival_count(invocation->startup_arrival_count);
        // Every CTA has finished using these fields, and StrongStream
        // prevents the next invocation from reusing them until this kernel
        // exits. No release ordering is needed for these reset stores.
        failure_latched.store(0, cuda::memory_order_relaxed);
        completion_arrival_count.store(0, cuda::memory_order_relaxed);
        startup_complete.store(0, cuda::memory_order_relaxed);
        startup_arrival_count.store(0, cuda::memory_order_relaxed);
    }
}

}  // namespace detail

// Run preparation on the startup CTA and share its result with every CTA.
template <CollectiveExecution Execution, typename Plan>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
beginCollectiveInvocation(Plan* plan,
                          const CollectiveRuntimeBindings& collective,
                          cooperative_groups::thread_block block) {
    if constexpr (Execution == CollectiveExecution::SingleCTA) {
        return detail::prepareCollectiveInvocation(plan, collective, block);
    } else {
        return detail::beginMultiCTAInvocation(plan, collective, block);
    }
}

// Called once per CTA after its last access to algorithm state.
template <CollectiveExecution Execution>
__device__ __forceinline__ void finishCollectiveInvocation(
    const CollectiveRuntimeBindings& collective,
    cooperative_groups::thread_block block, CollectiveResult result = {},
    int32_t* failed_ranks_hint = nullptr) {
    if constexpr (Execution == CollectiveExecution::SingleCTA) {
        if (result.succeeded()) return;
        block.sync();
        detail::recoverCollectiveFailure(
            collective, result, reinterpret_cast<uint64_t>(failed_ranks_hint),
            block);
    } else {
        detail::finishMultiCTAInvocation(collective, block, result,
                                         failed_ranks_hint);
    }
}

// Every CTA enters and leaves through the same invocation protocol, including
// CTAs with no channel work and zero-length requests.
template <CollectiveExecution Execution, typename Request, typename Plan,
          typename RunChannel>
__device__ __forceinline__ void runCollectiveInvocation(
    const Request& request, Plan* plan,
    const CollectiveRuntimeBindings& collective, RunChannel run_channel) {
    const auto block = cooperative_groups::this_thread_block();
    auto result = beginCollectiveInvocation<Execution>(plan, collective, block);
    if (result.succeeded()) {
        const auto& view = *collective.group_view;
        if (view.participant_count == 1) {
            executeLocalOnlyCollective(request, view.self().in_group_rank,
                                       block);
        } else {
            result = run_channel(block);
        }
    }
    finishCollectiveInvocation<Execution>(collective, block, result,
                                          request.failed_ranks_hint);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
