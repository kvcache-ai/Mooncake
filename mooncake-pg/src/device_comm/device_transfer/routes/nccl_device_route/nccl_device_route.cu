#ifdef USE_NCCL_DEVICE
#include <transport/device/nccl_device.cuh>

namespace mooncake {
namespace {

__global__ void drainGinKernel(const device::NcclDeviceContext* context,
                               int num_ranks, uint64_t timeout_ticks) {
    const auto context_index = blockIdx.x;
    device::NcclGinHandle gin(*context, context_index);
    const auto start_ticks = clock64();
    for (int nccl_rank = 0; nccl_rank < num_ranks; ++nccl_rank) {
        const auto elapsed_ticks = clock64() - start_ticks;
        if (timeout_ticks != 0 && elapsed_ticks >= timeout_ticks) {
            printf("[PG] NCCL device context %u drain timed out; continuing\n",
                   context_index);
            return;
        }
        const auto remaining_ticks =
            timeout_ticks == 0 ? 0 : timeout_ticks - elapsed_ticks;
        const auto status = gin.flushPeer<device::NcclGinTeam::kWorld>(
            nccl_rank, remaining_ticks);
        if (status != ncclSuccess) {
            printf(
                "[PG] NCCL device peer %d context %u drain failed "
                "(status=%d); continuing\n",
                nccl_rank, context_index, static_cast<int>(status));
        }
    }
}

}  // namespace

cudaError_t preloadNcclDeviceDrainKernel() {
    cudaFuncAttributes attributes{};
    return cudaFuncGetAttributes(&attributes, drainGinKernel);
}

cudaError_t launchNcclDeviceDrainKernel(
    const device::NcclDeviceContext* context, int num_ranks,
    int gin_context_count, uint64_t timeout_ticks, cudaStream_t stream) {
    drainGinKernel<<<gin_context_count, 1, 0, stream>>>(context, num_ranks,
                                                        timeout_ticks);
    return cudaGetLastError();
}

}  // namespace mooncake
#endif
