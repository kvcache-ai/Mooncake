#include "device_comm/device_collective/algorithms/star/star_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"

namespace mooncake {
namespace {

__global__ __launch_bounds__(kStarBlockThreads,
                             1) void starScatterKernel(ScatterRequest request,
                                                       uint64_t rank_bytes,
                                                       StarDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runStarSendChannel(request, rank_bytes, rank_bytes, state,
                                      blockIdx.x, block);
        });
}

}  // namespace

PGResult<void> launchStarScatterKernel(const ScatterRequest& request,
                                       StarDeviceState* state,
                                       cudaStream_t stream) {
    const uint64_t rank_bytes = request.count * elementSize(request.datatype);
    const dim3 grid(kMaxDeviceCollectiveChannels);
    starScatterKernel<<<grid, kStarBlockThreads, 0, stream>>>(
        request, rank_bytes, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
