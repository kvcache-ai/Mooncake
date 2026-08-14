#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH

#include <cstddef>
#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

inline constexpr uint32_t kMaxDeviceCollectiveChannels = kTransferLaneCount;
inline constexpr size_t kDeviceCollectiveTransferBufferSize =
    16ull << 20;  // 16 MiB per transfer buffer
inline constexpr size_t kDeviceCollectiveWorkspaceSize =
    2 * kDeviceCollectiveTransferBufferSize;  // 32 MiB total

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

// A byte-sized publication marker can be written directly by a control-stream
// memset, without a pinned host staging value.
enum class DevicePlanStatus : uint8_t {
    Unavailable = 0,
    Ready = 1,
};

template <typename Plan>
struct DevicePlanSlot {
    DevicePlanStatus status = DevicePlanStatus::Unavailable;
    Plan plan{};
};

// One algorithm-specific peer reference. global_rank selects a device-transfer
// route, while in_group_rank indexes collective protocol state; the offset
// addresses the peer communicator's control slice.
struct DeviceCollectivePeerTarget {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    uint64_t remote_control_offset = 0;
};

struct DeviceAllReducePlan {
    InGroupRank self_rank = kInvalidInGroupRank;
    int32_t self_active_index = -1;
    uint32_t participant_count = 0;

    DeviceCollectivePeerTarget predecessor;
    DeviceCollectivePeerTarget successor;
};

using DeviceAllReducePlanSlot = DevicePlanSlot<DeviceAllReducePlan>;

// The device publishes a new failure generation only after every active
// channel CTA has stopped touching the old Plan and shared buffers. Host
// publishes the matching ready generation after recovery has applied a new
// Plan.
struct alignas(64) DeviceCollectiveRecoveryMailbox {
    uint64_t failure_generation = 0;
    uint64_t ready_generation = 0;

    // The failure data below is valid only while failure_generation is newer
    // than ready_generation.
    InGroupRank failed_rank = 0;
    uint64_t failed_hint_address = 0;
};

// State shared by all channel CTAs in one kernel launch. A CTA increments
// arrived_channels after all of its threads have stopped using the Plan and
// transfer buffers; a non-last CTA then returns. If a failure was reported,
// the last CTA notifies the host and remains in the kernel until recovery
// finishes, then clears this state and returns.
struct alignas(64) DeviceCollectiveInvocationState {
    uint32_t arrived_channels = 0;
    uint32_t failure_latched = 0;
};

// Bound view of one [channel][peer] table in a Runtime control slice. The two
// offsets locate the table relative to the local service region and to a
// peer's control slice respectively. `max_group_size` is the number of entries
// in each channel row.
struct DeviceCollectiveSignalTable {
    uint64_t local_region_offset = 0;
    uint64_t control_offset = 0;
    uint32_t max_group_size = 0;

    __device__ __forceinline__ uint64_t
    localSlotOffset(uint32_t channel_index, InGroupRank peer_rank) const {
        return local_region_offset +
               slotIndex(channel_index, peer_rank) * sizeof(uint64_t);
    }

    __device__ __forceinline__ uint64_t
    remoteSlotOffset(uint32_t channel_index, InGroupRank peer_rank) const {
        return control_offset +
               slotIndex(channel_index, peer_rank) * sizeof(uint64_t);
    }

   private:
    // The row layout is an implementation detail of the bound table, not part
    // of the collective kernel's addressing logic.
    __device__ __forceinline__ uint64_t slotIndex(uint32_t channel_index,
                                                  InGroupRank peer_rank) const {
        return static_cast<uint64_t>(channel_index) * max_group_size +
               static_cast<uint32_t>(peer_rank);
    }
};

// Typed view of one allocated control slice. The layout records only byte
// offsets; this view resolves them into the pointers and signal-table
// descriptors consumed by host publication and collective kernels.
struct DeviceCollectiveControlView {
    DeviceAllReducePlanSlot* all_reduce_plan = nullptr;
    uint64_t* next_step_sequences = nullptr;
    uint64_t* next_recv_ready_sequences = nullptr;
    DeviceCollectiveInvocationState* invocation = nullptr;

    DeviceCollectiveSignalTable recv_ready_slots;
    DeviceCollectiveSignalTable signal_slots;
    DeviceCollectiveSignalTable consumed_ack_slots;
};

struct DeviceCollectiveTransferBuffer {
    void* addr = nullptr;
    uint64_t region_offset = 0;
    uint64_t size = 0;
};

struct DeviceCollectiveKernelResources {
    const DeviceTransferHandle* transfer_handle = nullptr;
    DeviceCollectiveTransferBuffer send_buffer;
    DeviceCollectiveTransferBuffer recv_buffer;
    uint64_t timeout_ticks = 0;

    DeviceCollectiveControlView control;
    DeviceCollectiveRecoveryMailbox* recovery = nullptr;
};

struct DeviceAllReduceKernelArgs {
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    uint32_t channel_count = 1;
    int32_t* failed_ranks_hint = nullptr;
};

cudaError_t launchDeviceAllReduceKernel(
    const DeviceAllReduceKernelArgs& request,
    const DeviceCollectiveKernelResources& resources, cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_TYPES_CUH
