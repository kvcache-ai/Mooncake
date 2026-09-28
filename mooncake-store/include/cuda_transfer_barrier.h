#pragma once

#ifdef USE_CUDA

#include <cuda_runtime_api.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "transfer_task.h"

namespace mooncake {

class Client;

// Bridges a host-completed transfer to a CUDA stream wait. Keep the destination
// and this barrier alive until graph execution finishes. For a direct
// TransferFuture, keep its Store lease valid until the transfer completes.
// A failed transfer still releases the wait; discard graph output on failure.
class CudaTransferBarrier {
   public:
    static std::unique_ptr<CudaTransferBarrier> Create();
    ~CudaTransferBarrier();

    CudaTransferBarrier(const CudaTransferBarrier&) = delete;
    CudaTransferBarrier& operator=(const CudaTransferBarrier&) = delete;

    // Capture this once in a graph, then call start() before each replay.
    // Finish the preceding replay before rearming this barrier.
    bool enqueueWait(cudaStream_t stream);
    bool start(TransferFuture future);
    bool start(std::function<ErrorCode()> operation);
    std::optional<ErrorCode> result() const;
    ErrorCode wait();

   private:
    CudaTransferBarrier(uint32_t* host_flag, void* device_flag, int device)
        : host_flag_(host_flag), device_flag_(device_flag), device_(device) {}

    uint32_t* host_flag_;
    void* device_flag_;
    int device_;
    std::thread worker_;
    ErrorCode result_ = ErrorCode::INVALID_PARAMS;
    bool started_ = false;
};

// Runs the ordinary Store read in the background, preserving its replica and
// lease handling. The caller must keep the destination slices alive until
// wait() completes and discard graph output if the read fails.
bool StartBatchGetWithCudaBarrier(
    std::shared_ptr<Client> client, std::vector<std::string> keys,
    std::unordered_map<std::string, std::vector<Slice>> slices,
    CudaTransferBarrier& barrier);

}  // namespace mooncake

#endif
