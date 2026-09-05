// Host-callable CUDA launch wrappers for the RDMA route.
// Definitions containing <<<>>> must be CUDA-compiled, so they live in this
// .cu while host .cpp callers include only declarations from rdma_route.h.

#include "device_comm/device_transfer/routes/rdma_route/rdma_route.cuh"

namespace mooncake {

void launchRdmaDrainKernel(void* qp_devctxs, uint32_t num_qps,
                           uint64_t timeout_ticks, TransferResult* results,
                           cudaStream_t stream) {
    constexpr uint32_t kThreads = 128;
    // Round up to satisfy blocks * kThreads >= num_qps (one thread per QP).
    const uint32_t blocks = (num_qps + kThreads - 1) / kThreads;
    drainRdmaQueuePairs<<<blocks, kThreads, 0, stream>>>(
        static_cast<mlx5gda_qp_devctx*>(qp_devctxs), num_qps, timeout_ticks,
        results);
}

}  // namespace mooncake
