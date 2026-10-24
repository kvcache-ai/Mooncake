#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH

#include <cooperative_groups.h>
#include <cstdint>

#include "pg_assert.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"
#include "device_comm/device_utils/h2d_request_slot.cuh"

namespace mooncake {
namespace detail {

// Split the CTA between words of a fill value and independent elements.
// Each thread loads a word once and reuses it for its assigned elements.
template <typename T>
__device__ __forceinline__ void fillControlUpdateValues(
    const ControlUpdateOp::Fill& fill, const uint8_t* payload,
    cooperative_groups::thread_block block) {
    const auto* source =
        reinterpret_cast<const volatile T*>(payload + fill.payload_offset);
    auto* destination = reinterpret_cast<volatile T*>(fill.destination);
    const uint32_t value_count = fill.element_size / sizeof(T);
    const uint32_t threads_per_element =
        value_count < block.size() ? value_count : block.size();
    const uint32_t elements_per_block = block.size() / threads_per_element;
    const uint32_t first_element = block.thread_rank() / threads_per_element;
    if (first_element >= elements_per_block || first_element >= fill.count)
        return;

    for (uint32_t index = block.thread_rank() % threads_per_element;
         index < value_count; index += threads_per_element) {
        const T value = source[index];
        for (uint64_t element = first_element; element < fill.count;
             element += elements_per_block) {
            destination[element * value_count + index] = value;
        }
    }
}

__device__ __forceinline__ void executeClaimedControlUpdate(
    const ControlMailbox::ControlUpdateSlot::ReceivedRequest& received,
    cooperative_groups::thread_block block) {
    __shared__ const ControlUpdate* shared_update;
    if (block.thread_rank() == 0) shared_update = received.request;
    block.sync();
    if (!shared_update) return;

    const auto& update = *shared_update;
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
                copyValuesTo(update.payload + copy.payload_offset, copy.size,
                             block,
                             reinterpret_cast<uint8_t*>(copy.destination));
                break;
            }
            case ControlUpdateOpKind::Fill: {
                const auto fill = operation.payload.fill;
                PG_ASSERT(fill.destination != 0 && fill.element_size != 0 &&
                          fill.payload_offset <= payload_size &&
                          fill.element_size <=
                              payload_size - fill.payload_offset);
                // Use 64-bit accesses when both addresses are 8-byte aligned
                // and element_size is a multiple of 8.
                if (((fill.destination | fill.payload_offset |
                      fill.element_size) &
                     (sizeof(uint64_t) - 1)) == 0) {
                    fillControlUpdateValues<uint64_t>(fill, update.payload,
                                                      block);
                } else {
                    fillControlUpdateValues<uint8_t>(fill, update.payload,
                                                     block);
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
        block.sync();
    }

    // All threads must finish reading the request before reply releases it,
    // including when the update contains no operations.
    block.sync();
    if (block.thread_rank() == 0) received.handle.reply({});
    block.sync();
}

}  // namespace detail

// Called by the startup CTA before any CTA reads algorithm state.
__device__ __forceinline__ void applyPendingControlUpdate(
    ControlMailbox::ControlUpdateSlot* slot,
    cooperative_groups::thread_block block) {
    ControlMailbox::ControlUpdateSlot::ReceivedRequest received;
    if (block.thread_rank() == 0 && slot->tryReceive(received)) {
        PG_ASSERT(!received.pinned);
    }
    detail::executeClaimedControlUpdate(received, block);
}

// Called only after recovery is acknowledged. The host pins the update before
// replying, so a pinned request must be available.
__device__ __forceinline__ void applyPinnedControlUpdate(
    ControlMailbox::ControlUpdateSlot* slot,
    cooperative_groups::thread_block block) {
    ControlMailbox::ControlUpdateSlot::ReceivedRequest received;
    if (block.thread_rank() == 0) {
        const bool has_update = slot->tryReceive(received);
        PG_ASSERT(has_update && received.pinned);
    }
    detail::executeClaimedControlUpdate(received, block);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_CUH
