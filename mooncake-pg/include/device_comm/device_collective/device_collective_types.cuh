#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

inline constexpr uint32_t kMaxDeviceCollectiveChannels = 32;
static_assert(kMaxDeviceCollectiveChannels <= kTransferLaneCount);

inline constexpr bool isDeviceAllReduceCombinationSupported(
    DataType datatype, ReduceOp op) noexcept {
    switch (op) {
        case ReduceOp::Sum:
        case ReduceOp::Product:
        case ReduceOp::Min:
        case ReduceOp::Max:
            break;
        default:
            return false;
    }
    switch (datatype) {
        case DataType::Float16:
            return op == ReduceOp::Sum;
        case DataType::Uint8:
        case DataType::Int8:
        case DataType::Int16:
        case DataType::Int32:
        case DataType::Int64:
        case DataType::Bfloat16:
        case DataType::Float32:
        case DataType::Float64:
        case DataType::Bool:
            return true;
        default:
            return false;
    }
}

enum class DevicePlanStatus : uint8_t {
    Unavailable = 0,
    Ready = 1,
};

template <typename Plan>
struct DevicePlanSlot {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    Plan plan{};
};

// The device publishes a new failure generation only after every active
// channel CTA has stopped touching the old Plan and protocol buffers. The host
// publishes the matching ready generation after recovery has installed the
// replacement Plan.
struct alignas(64) DeviceCollectiveRecoveryMailbox {
    uint64_t failure_generation = 0;
    uint64_t ready_generation = 0;

    // Valid only while failure_generation is newer than ready_generation.
    InGroupRank failed_rank = 0;
    uint64_t failed_hint_address = 0;
};

// State shared by all channel CTAs in one collective launch. A CTA increments
// arrived_channels after all of its threads have stopped using the Plan and
// protocol buffers; a non-last CTA then returns. If a failure was reported,
// the last CTA publishes the latched metadata to the host recovery mailbox and
// remains in the kernel until recovery finishes.
struct alignas(64) DeviceCollectiveInvocationState {
    uint32_t arrived_channels = 0;
    uint32_t failure_latched = 0;
    InGroupRank failed_rank = kInvalidInGroupRank;
    uint64_t failed_hint_address = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
