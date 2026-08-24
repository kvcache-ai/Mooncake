#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH

#include <cooperative_groups.h>
#include <transport/device/device_ops.cuh>

#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

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
        auto* const arrived =
            reinterpret_cast<unsigned int*>(&invocation->arrived_channels);

        if (detected_failed_rank != kInvalidInGroupRank &&
            atomicCAS(failed, 0u, 1u) == 0u) {
            invocation->failed_rank = detected_failed_rank;
            invocation->failed_hint_address =
                reinterpret_cast<uint64_t>(failed_ranks_hint);
        }

        // Order this CTA's prior writes before its arrived_channels
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
                // arrived_channels. This CTA observed the final increment;
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
            }

            atomicExch(failed, 0u);
            __threadfence();
            atomicExch(arrived, 0u);
        }
    }
    block.sync();
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_KERNEL_CUH
