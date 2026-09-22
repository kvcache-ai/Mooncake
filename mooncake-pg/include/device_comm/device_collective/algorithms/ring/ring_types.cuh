#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_TYPES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_TYPES_CUH

#include <cstdint>

#include <cuda_alike.h>

#include "common_types.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"

namespace mooncake {

// An oversized chunk reduces pipeline overlap, so a cap is applied here.

inline constexpr uint64_t kMaxRingChunkBytes = 512ull * 1024;  // 512 KiB

// Shared by host resource layouts and device Simple primitives.
inline constexpr uint32_t kRingPipelineDepth = kDefaultSimplePipelineDepth;

// Complete published resource and topology binding read by a Ring AllReduce
// kernel. Rolling sequence state belongs to the communicator's Simple
// resources. A control update replaces the Plan while algorithm execution is
// quiescent, so a kernel needs only the state pointer and its per-invocation
// request.
struct RingAllReducePlan {
    const DeviceTransferHandle* transfer_handle = nullptr;
    uint64_t timeout_ticks = 0;
    uint64_t view_epoch = kInvalidViewEpoch;

    // Local workspace and its send destination in the successor's DTS region.
    char* buffer_ptr = nullptr;
    uint64_t buffer_size = 0;
    uint64_t send_buffer_offset = 0;

    // Optional source binding in DTS's separate local staging allocation. It
    // is never published to peers.
    char* staging_ptr = nullptr;
    uint64_t staging_size = 0;

    int32_t self_active_index = -1;
    uint32_t participant_count = 0;
    InGroupRank predecessor = kInvalidInGroupRank;
    InGroupRank successor = kInvalidInGroupRank;
};

using RingAllReducePlanSlot = PlanSlot<RingAllReducePlan>;

// Ordinary device memory owned by one Ring AllReduce instance. None
// of this state is registered or published to peers.
struct alignas(256) RingAllReduceDeviceState {
    RingAllReducePlanSlot plan;
    // Runtime-owned local slice; word i is written only by InGroupRank i.
    const uint64_t* view_epoch_signals = nullptr;
    InvocationState* invocation_state = nullptr;
    ControlMailbox* control_mailbox = nullptr;
    // Borrowed peer bindings and progress, shared with other Simple algorithms.
    SimpleState<kRingPipelineDepth>* simple = nullptr;
};

struct RingAllReduceKernelArgs {
    const void* send_buffer = nullptr;
    void* recv_buffer = nullptr;
    uint64_t count = 0;
    DataType datatype = DataType::Float32;
    ReduceOp op = ReduceOp::Sum;
    int32_t* failed_ranks_hint = nullptr;
};

cudaError_t launchRingAllReduceKernel(const RingAllReduceKernelArgs& request,
                                      RingAllReduceDeviceState* state,
                                      cudaStream_t stream);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_RING_TYPES_CUH
