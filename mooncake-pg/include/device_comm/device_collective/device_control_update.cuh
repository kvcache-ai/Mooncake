#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH

#include <cstdint>

#include <cuda/atomic>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {
namespace detail {

__device__ __forceinline__ void executeClaimedControlUpdate(
    ControlUpdateSlot* slot) {
    const auto& update = slot->update;
    const uint32_t operation_count = update.operation_count;
    const uint32_t payload_size = update.payload_size;
    PG_DEVICE_ASSERT(operation_count <= kMaxDeviceControlUpdateOperations);
    PG_DEVICE_ASSERT(payload_size <= kDeviceControlUpdatePayloadBytes);

    for (uint32_t operation_index = 0; operation_index < operation_count;
         ++operation_index) {
        const auto operation = update.operations[operation_index];
        switch (operation.kind) {
            case ControlUpdateOpKind::CopyBytes: {
                const auto copy = operation.payload.copy_bytes;
                PG_DEVICE_ASSERT(copy.destination != 0 &&
                                 copy.payload_offset <= payload_size &&
                                 copy.size <=
                                     payload_size - copy.payload_offset);
                auto* const destination =
                    reinterpret_cast<volatile uint8_t*>(copy.destination);
                const auto* const source =
                    reinterpret_cast<const volatile uint8_t*>(
                        update.payload + copy.payload_offset);
                for (uint32_t index = 0; index < copy.size; ++index) {
                    destination[index] = source[index];
                }
                break;
            }
            case ControlUpdateOpKind::FillBytes: {
                const auto fill = operation.payload.fill_bytes;
                PG_DEVICE_ASSERT(fill.destination != 0);
                auto* const destination =
                    reinterpret_cast<volatile uint8_t*>(fill.destination);
                for (uint32_t index = 0; index < fill.count; ++index) {
                    destination[index] = fill.value;
                }
                break;
            }
            case ControlUpdateOpKind::FillU64: {
                const auto fill = operation.payload.fill_u64;
                PG_DEVICE_ASSERT(fill.destination != 0);
                auto* const destination =
                    reinterpret_cast<volatile uint64_t*>(fill.destination);
                for (uint32_t index = 0; index < fill.count; ++index) {
                    destination[index] = fill.value;
                }
                break;
            }
            default:
                // ControlUpdateBuilder is the only producer and emits only
                // the operation kinds handled above.
                PG_DEVICE_UNREACHABLE();
        }

        // Operations are ordered. In particular, a Plan cannot become Ready
        // before its state reset and contents are globally visible.
        __threadfence_system();
    }

    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> state(slot->state);
    state.store(static_cast<uint32_t>(ControlUpdateState::Idle),
                cuda::memory_order_release);
}

}  // namespace detail

// Called by the elected first resident CTA before any CTA reads protocol state.
// Publication constructs the batch before taking Writing, so a device that
// loses the state CAS can safely wait for it and then claim the newly published
// complete update.
__device__ __forceinline__ void applyPendingControlUpdate(
    ControlUpdateSlot* slot) {
    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> state(slot->state);
    while (true) {
        uint32_t observed = state.load(cuda::memory_order_acquire);
        const auto observed_state = static_cast<ControlUpdateState>(observed);
        switch (observed_state) {
            case ControlUpdateState::Idle:
                return;
            case ControlUpdateState::Writing:
                continue;
            case ControlUpdateState::Published:
                if (!state.compare_exchange_strong(
                        observed,
                        static_cast<uint32_t>(ControlUpdateState::Claimed),
                        cuda::memory_order_acquire,
                        cuda::memory_order_relaxed)) {
                    continue;
                }
                detail::executeClaimedControlUpdate(slot);
                return;
            case ControlUpdateState::Pinned:
            case ControlUpdateState::Claimed:
                // StrongStream excludes another collective invocation, and a
                // failed invocation consumes Pinned before its successor
                // starts.
                PG_DEVICE_UNREACHABLE();
                return;
            default:
                PG_DEVICE_UNREACHABLE();
                return;
        }
    }
}

// Called only after the recovery worker acknowledges a failure. The update was
// pinned before that acknowledgement, so no ordinary collective may consume
// or replace it.
__device__ __forceinline__ void applyPinnedControlUpdate(
    ControlUpdateSlot* slot) {
    cuda::atomic_ref<uint32_t, cuda::thread_scope_system> state(slot->state);
    uint32_t observed = static_cast<uint32_t>(ControlUpdateState::Pinned);
    if (!state.compare_exchange_strong(
            observed, static_cast<uint32_t>(ControlUpdateState::Claimed),
            cuda::memory_order_acquire, cuda::memory_order_relaxed)) {
        // The worker acknowledges recovery only after the host pins this
        // update. Host writers cannot replace Pinned, and StrongStream excludes
        // another device claimant, so this CAS must succeed.
        PG_DEVICE_UNREACHABLE();
        return;
    }
    detail::executeClaimedControlUpdate(slot);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH
