#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH

#include <cstdint>

#include "pg_assert.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_utils/h2d_request_slot.cuh"

namespace mooncake {
namespace detail {

__device__ __forceinline__ void executeClaimedControlUpdate(
    const ControlMailbox::ControlUpdateSlot::ReceivedRequest& received) {
    const auto& update = *received.request;
    const uint32_t operation_count = update.operation_count;
    const uint32_t payload_size = update.payload_size;
    PG_ASSERT(operation_count <= kMaxDeviceControlUpdateOperations);
    PG_ASSERT(payload_size <= kDeviceControlUpdatePayloadBytes);

    for (uint32_t operation_index = 0; operation_index < operation_count;
         ++operation_index) {
        const auto operation = update.operations[operation_index];
        switch (operation.kind) {
            case ControlUpdateOpKind::CopyBytes: {
                const auto copy = operation.payload.copy_bytes;
                PG_ASSERT(copy.destination != 0 &&
                          copy.payload_offset <= payload_size &&
                          copy.size <= payload_size - copy.payload_offset);
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
                PG_ASSERT(fill.destination != 0);
                auto* const destination =
                    reinterpret_cast<volatile uint8_t*>(fill.destination);
                for (uint32_t index = 0; index < fill.count; ++index) {
                    destination[index] = fill.value;
                }
                break;
            }
            case ControlUpdateOpKind::FillU64: {
                const auto fill = operation.payload.fill_u64;
                PG_ASSERT(fill.destination != 0);
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
                PG_UNREACHABLE();
        }

        // Operations are ordered. In particular, a Plan cannot become Ready
        // before its state reset and contents are globally visible.
        __threadfence_system();
    }

    received.handle.reply({});
}

}  // namespace detail

// Called by the startup thread before any CTA reads algorithm state.
__device__ __forceinline__ void applyPendingControlUpdate(
    ControlMailbox::ControlUpdateSlot* slot) {
    ControlMailbox::ControlUpdateSlot::ReceivedRequest received;
    if (!slot->tryReceive(received)) return;
    PG_ASSERT(!received.pinned);
    detail::executeClaimedControlUpdate(received);
}

// Called only after recovery is acknowledged. The host pins the update before
// replying, so a pinned request must be available.
__device__ __forceinline__ void applyPinnedControlUpdate(
    ControlMailbox::ControlUpdateSlot* slot) {
    ControlMailbox::ControlUpdateSlot::ReceivedRequest received;
    if (!slot->tryReceive(received)) {
        PG_UNREACHABLE();
        return;
    }
    PG_ASSERT(received.pinned);
    detail::executeClaimedControlUpdate(received);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH
