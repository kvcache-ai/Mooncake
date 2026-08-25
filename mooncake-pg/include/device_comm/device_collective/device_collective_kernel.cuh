#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH

#include <cooperative_groups.h>
#include <cuda/atomic>
#include <transport/device/device_ops.cuh>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_collective/device_control_update.cuh"
#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

// Elect the first resident CTA, let its first thread apply one pending update,
// and hold every other CTA before it reads protocol state. Electing by arrival
// rather than block index avoids a scheduling deadlock when block 0 is not yet
// resident. StrongStream prevents different collective invocations from
// overlapping on this reusable state.
__device__ __forceinline__ void prepareCollectiveInvocation(
    DeviceCollectiveInvocationState* invocation,
    DeviceCollectiveRecoveryMailbox* recovery_mailbox,
    cooperative_groups::thread_block block) {
    __shared__ uint32_t is_startup_leader;
    if (block.thread_rank() == 0) {
        auto* const startup_arrival_count =
            reinterpret_cast<unsigned int*>(&invocation->startup_arrival_count);
        is_startup_leader = atomicAdd(startup_arrival_count, 1u) == 0u;
    }
    block.sync();

    if (block.thread_rank() == 0) {
        cuda::atomic_ref<uint32_t, cuda::thread_scope_device> startup_complete(
            invocation->startup_complete);
        if (is_startup_leader != 0) {
            applyPendingControlUpdate(&recovery_mailbox->control_update_slot);
            startup_complete.store(1, cuda::memory_order_release);
        } else {
            while (startup_complete.load(cuda::memory_order_acquire) == 0) {
            }
        }
    }
    block.sync();
}

// Completes this channel after success or a locally detected failure. Only a
// detecting channel supplies a failed rank; the first detector records the
// failure metadata.
__device__ __forceinline__ void completeChannel(
    DeviceCollectiveInvocationState* invocation,
    DeviceCollectiveRecoveryMailbox* recovery_mailbox,
    cooperative_groups::thread_block block,
    InGroupRank detected_failed_rank = kInvalidInGroupRank,
    int32_t* failed_ranks_hint = nullptr) {
    // No thread may publish channel completion while another thread in the CTA
    // can still access the current Plan or protocol buffers.
    block.sync();
    if (block.thread_rank() == 0) {
        auto* const failed =
            reinterpret_cast<unsigned int*>(&invocation->failure_latched);
        auto* const arrived = reinterpret_cast<unsigned int*>(
            &invocation->completion_arrival_count);

        if (detected_failed_rank != kInvalidInGroupRank &&
            atomicCAS(failed, 0u, 1u) == 0u) {
            invocation->failed_rank = detected_failed_rank;
            invocation->failed_hint_address =
                reinterpret_cast<uint64_t>(failed_ranks_hint);
        }

        // Order this CTA's prior writes before its completion-arrival
        // increment. All CTAs update the same counter, so the CTA that
        // observes the final increment knows every channel has stopped
        // touching the current Plan and protocol buffers.
        __threadfence();
        const uint32_t previous = atomicAdd(arrived, 1u);
        if (previous + 1 == gridDim.x) {
            if (atomicAdd(failed, 0u) != 0u) {
                const uint64_t generation =
                    device::mc_ld_acquire_u64(
                        &recovery_mailbox->failure_generation) +
                    1;

                recovery_mailbox->failed_rank = invocation->failed_rank;
                recovery_mailbox->failed_hint_address =
                    invocation->failed_hint_address;

                // Every channel ordered its prior writes before incrementing
                // completion_arrival_count. This CTA observed the final
                // increment;
                // make the copied failure metadata system-visible before the
                // release store notifies the host. The matching acquire load
                // lets the host read the metadata and replace the Plan and
                // protocol state only after all channels are quiescent.
                __threadfence_system();
                device::mc_st_release_u64(&recovery_mailbox->failure_generation,
                                          generation);
                while (device::mc_ld_acquire_u64(
                           &recovery_mailbox->ready_generation) < generation) {
                }
                applyPinnedControlUpdate(
                    &recovery_mailbox->control_update_slot);
            }

            atomicExch(failed, 0u);
            __threadfence();
            atomicExch(arrived, 0u);
            atomicExch(
                reinterpret_cast<unsigned int*>(&invocation->startup_complete),
                0u);
            __threadfence();
            atomicExch(reinterpret_cast<unsigned int*>(
                           &invocation->startup_arrival_count),
                       0u);
        }
    }
    block.sync();
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
