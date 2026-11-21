#include "device_comm/device_collective/device_control_update.h"

#include <cstring>
#include <limits>

namespace mooncake {

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

PGResult<void> ControlUpdateBuilder::fillPattern(void* destination,
                                                 const void* value,
                                                 size_t element_size,
                                                 size_t count) {
    PG_VALIDATE_ARG(destination, "control update fill destination is null");
    PG_VALIDATE_ARG(count <= std::numeric_limits<uint32_t>::max(),
                    "control update fill is too large");
    PG_VALIDATE_STATE(
        element_size <= kDeviceControlUpdatePayloadBytes - update_.payload_size,
        "device control update payload is too large");
    const uint32_t payload_offset = update_.payload_size;
    std::memcpy(update_.payload + payload_offset, value, element_size);
    update_.payload_size = payload_offset + static_cast<uint32_t>(element_size);
    ControlUpdateOp operation;
    operation.kind = ControlUpdateOpKind::Fill;
    operation.payload.fill = ControlUpdateOp::Fill{
        .destination = reinterpret_cast<uint64_t>(destination),
        .element_size = static_cast<uint32_t>(element_size),
        .count = static_cast<uint32_t>(count),
        .payload_offset = payload_offset,
    };
    return append(operation);
}

}  // namespace mooncake
