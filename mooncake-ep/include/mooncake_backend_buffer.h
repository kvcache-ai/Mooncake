#ifndef MOONCAKE_BACKEND_BUFFER_H
#define MOONCAKE_BACKEND_BUFFER_H

#include <cstdlib>
#include <cuda_runtime.h>
#include <torch/csrc/distributed/c10d/Utils.hpp>

namespace mooncake {

constexpr size_t kBufferSize = 1u << 29;
constexpr size_t kMaxNumRanks = 64;

struct BackendBuffer {
    void* cpuSendBuffer_[2];
    void* cpuRecvBuffer_[2];
    void* cudaSendBuffer_[2];
    void* cudaRecvBuffer_[2];
    int32_t* cpuSyncSendRegion_[2];
    int32_t* cpuSyncRecvRegion_[2];
    int32_t* cudaSyncSendRegion_[2];
    int32_t* cudaSyncRecvRegion_[2];
    int cpuTaskCount_ = 0;
    int cudaTaskCount_ = 0;

    BackendBuffer() {
        for (size_t i = 0; i < 2; i++) {
            cpuSendBuffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(cpuSendBuffer_[i],
                        c10::str("Failed to allocate CPU send buffer"));

            cpuRecvBuffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(cpuRecvBuffer_[i],
                        c10::str("Failed to allocate CPU recv buffer"));

            cudaError err = cudaMalloc(&cudaSendBuffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

            err = cudaMalloc(&cudaRecvBuffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

            cpuSyncSendRegion_[i] = new int32_t[kMaxNumRanks]{};
            cpuSyncRecvRegion_[i] = new int32_t[kMaxNumRanks]{};

            cudaSyncSendRegion_[i] = new int32_t[kMaxNumRanks]{};
            cudaSyncRecvRegion_[i] = new int32_t[kMaxNumRanks]{};
        }
    }

    ~BackendBuffer() {
        for (size_t i = 0; i < 2; i++) {
            free(cpuSendBuffer_[i]);
            free(cpuRecvBuffer_[i]);
            cudaFree(cudaSendBuffer_[i]);
            cudaFree(cudaRecvBuffer_[i]);
            delete[] cpuSyncSendRegion_[i];
            delete[] cpuSyncRecvRegion_[i];
            delete[] cudaSyncSendRegion_[i];
            delete[] cudaSyncRecvRegion_[i];
        }
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_BACKEND_BUFFER_H
