#include "device_comm/device_collective/device_control_update.h"

#include <atomic>
#include <cstring>
#include <limits>
#include <thread>

namespace mooncake {
namespace {

uint32_t stateValue(ControlUpdateState state) noexcept {
    return static_cast<uint32_t>(state);
}

}  // namespace

PGResult<void> ControlUpdateBuilder::append(ControlUpdateOp operation) {
    PG_VALIDATE_STATE(
        update_.operation_count < kMaxDeviceControlUpdateOperations,
        "device control update has too many operations");
    update_.operations[update_.operation_count++] = operation;
    return {};
}

PGResult<void> ControlUpdateBuilder::copyBytes(void* destination,
                                               const void* source,
                                               size_t size) {
    PG_VALIDATE_ARG(destination, "control update copy destination is null");
    PG_VALIDATE_ARG(source || size == 0, "control update copy source is null");
    PG_VALIDATE_STATE(
        size <= kDeviceControlUpdatePayloadBytes - update_.payload_size,
        "device control update payload is too large");
    const uint32_t payload_offset = update_.payload_size;
    if (size != 0) {
        std::memcpy(update_.payload + payload_offset, source, size);
    }
    update_.payload_size = payload_offset + static_cast<uint32_t>(size);
    ControlUpdateOp operation;
    operation.kind = ControlUpdateOpKind::CopyBytes;
    operation.payload.copy_bytes = ControlUpdateOp::CopyBytes{
        .destination = reinterpret_cast<uint64_t>(destination),
        .size = static_cast<uint32_t>(size),
        .payload_offset = payload_offset,
    };
    return append(operation);
}

PGResult<void> ControlUpdateBuilder::fillBytes(void* destination, uint8_t value,
                                               size_t count) {
    PG_VALIDATE_ARG(destination, "control update fill destination is null");
    PG_VALIDATE_ARG(count <= std::numeric_limits<uint32_t>::max(),
                    "control update fill is too large");
    ControlUpdateOp operation;
    operation.kind = ControlUpdateOpKind::FillBytes;
    operation.payload.fill_bytes = ControlUpdateOp::FillBytes{
        .destination = reinterpret_cast<uint64_t>(destination),
        .value = value,
        .count = static_cast<uint32_t>(count),
    };
    return append(operation);
}

PGResult<void> ControlUpdateBuilder::fillU64(uint64_t* destination,
                                             uint64_t value, size_t count) {
    PG_VALIDATE_ARG(destination, "control update fill destination is null");
    PG_VALIDATE_ARG(count <= std::numeric_limits<uint32_t>::max(),
                    "control update fill is too large");
    ControlUpdateOp operation;
    operation.kind = ControlUpdateOpKind::FillU64;
    operation.payload.fill_u64 = ControlUpdateOp::FillU64{
        .destination = reinterpret_cast<uint64_t>(destination),
        .value = value,
        .count = static_cast<uint32_t>(count),
    };
    return append(operation);
}

void publishControlUpdate(ControlUpdateSlot& slot, const ControlUpdate& update,
                          bool pinned) {
    const auto published_state =
        pinned ? ControlUpdateState::Pinned : ControlUpdateState::Published;
    auto state = std::atomic_ref(slot.state);
    while (true) {
        const auto observed = static_cast<ControlUpdateState>(
            state.load(std::memory_order_acquire));
        switch (observed) {
            case ControlUpdateState::Idle:
            case ControlUpdateState::Published: {
                uint32_t expected = stateValue(observed);
                if (!state.compare_exchange_strong(
                        expected, stateValue(ControlUpdateState::Writing),
                        std::memory_order_acq_rel, std::memory_order_acquire)) {
                    continue;
                }
                std::memcpy(&slot.update, &update, sizeof(update));
                state.store(stateValue(published_state),
                            std::memory_order_release);
                return;
            }
            case ControlUpdateState::Pinned:
            case ControlUpdateState::Claimed:
                // Wait the device kernel.
                std::this_thread::yield();
                continue;
            case ControlUpdateState::Writing:
                PG_ASSERT(
                    false,
                    "another host publisher owns the control-update slot");
            default:
                PG_ASSERT(false, "invalid control-update state");
        }
    }
}

}  // namespace mooncake
