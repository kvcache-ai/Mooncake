#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_TYPES_CUH

#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/protocols/ll/ll_types.cuh"

namespace mooncake {

// Tuning cap for one exchange
inline constexpr uint32_t kMaxOneShotChunkBytes = 64 * 1024;
static_assert(kMaxOneShotChunkBytes > 0 &&
              kMaxOneShotChunkBytes % sizeof(uint32_t) == 0);
inline constexpr uint32_t kOneShotSlots = 2;
inline constexpr uint32_t kMaxOneShotChannels = 8;
static_assert(kMaxOneShotChannels <= kTransferLaneCount);

inline constexpr bool isOneShotAllReduceCombinationSupported(
    DataType datatype, ReduceOp op) noexcept {
    return isDeviceAllReduceCombinationSupported(datatype, op) &&
           (datatype == DataType::Float16 || datatype == DataType::Bfloat16 ||
            datatype == DataType::Float32 || datatype == DataType::Int32);
}

// Two exchange slots in the shared workspace. Sender indices are stable
// InGroupRank values; LL packets occupy twice the original payload bytes.
struct OneShotBufferLayout {
    // Payload capacity per [slot][sender], in bytes, excluding LL tags.
    uint64_t slot_capacity = 0;
    uint32_t max_group_size = 0;

    [[nodiscard]] __host__ __device__ static constexpr OneShotBufferLayout make(
        uint64_t buffer_bytes, uint32_t max_group_size) {
        const uint64_t available =
            buffer_bytes / (uint64_t{kOneShotSlots} * max_group_size * 2);
        return {available - available % sizeof(uint32_t), max_group_size};
    }

    // Index of a 64-bit LL packet within the payload workspace.
    [[nodiscard]] __host__ __device__ constexpr uint64_t packetIndex(
        uint32_t slot, InGroupRank sender, uint64_t word = 0) const {
        return (uint64_t{slot} * max_group_size +
                static_cast<uint32_t>(sender)) *
                   (slot_capacity / sizeof(uint32_t)) +
               word;
    }

    [[nodiscard]] __host__ __device__ constexpr uint64_t bufferBytes() const {
        return uint64_t{kOneShotSlots} * max_group_size * slot_capacity * 2;
    }
};

struct OneShotAllReducePlan {
    const DeviceTransferHandle* transfer_handle = nullptr;
    uint64_t timeout_ticks = 0;
    uint64_t view_epoch = kInvalidViewEpoch;
    char* buffer_ptr = nullptr;
    OneShotBufferLayout layout;
    // Common exchange size, capped by every participating peer's capacity.
    uint32_t chunk_bytes = 0;
    InGroupRank self_rank = kInvalidInGroupRank;
    uint32_t self_active_index = 0;
    uint32_t participant_count = 0;
    InGroupRank peers[kMaxNumRanks] = {};
    // Same active-peer order as peers[]. Each workspace has its own DTS offset.
    uint64_t peer_buffer_offsets[kMaxNumRanks] = {};
};

using OneShotAllReducePlanSlot = PlanSlot<OneShotAllReducePlan>;

struct alignas(64) OneShotAllReduceDeviceState {
    OneShotAllReducePlanSlot plan;
    const uint64_t* view_epoch_signals = nullptr;
    InvocationState* invocation_state = nullptr;
    ControlMailbox* control_mailbox = nullptr;
    // Borrowed peer bindings and progress from the communicator's LL resources.
    LLState* ll = nullptr;
};

struct OneShotAllReduceKernelArgs {
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    int32_t* failed_ranks_hint = nullptr;
};

cudaError_t launchOneShotAllReduceKernel(
    const OneShotAllReduceKernelArgs& request,
    OneShotAllReduceDeviceState* state, cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_TYPES_CUH
