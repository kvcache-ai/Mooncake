#include "cuda_transfer_barrier.h"

#ifdef USE_CUDA

#include <cuda.h>

#include <atomic>
#include <exception>
#include <utility>

#include <glog/logging.h>

#include "client_service.h"

namespace mooncake {

std::unique_ptr<CudaTransferBarrier> CudaTransferBarrier::Create() {
    if (cuInit(0) != CUDA_SUCCESS) return nullptr;
    int device = -1;
    if (cudaGetDevice(&device) != cudaSuccess) return nullptr;
    void* host_flag = nullptr;
    if (cudaHostAlloc(&host_flag, sizeof(uint32_t), cudaHostAllocMapped) !=
        cudaSuccess)
        return nullptr;
    void* device_flag = nullptr;
    if (cudaHostGetDevicePointer(&device_flag, host_flag, 0) != cudaSuccess) {
        cudaFreeHost(host_flag);
        return nullptr;
    }
    auto* flag = static_cast<uint32_t*>(host_flag);
    *flag = 0;
    return std::unique_ptr<CudaTransferBarrier>(
        new CudaTransferBarrier(flag, device_flag, device));
}

CudaTransferBarrier::~CudaTransferBarrier() {
    if (worker_.joinable()) worker_.join();
    cudaFreeHost(host_flag_);
}

bool CudaTransferBarrier::enqueueWait(cudaStream_t stream) {
    return cuStreamWaitValue32(reinterpret_cast<CUstream>(stream),
                               reinterpret_cast<CUdeviceptr>(device_flag_), 1,
                               CU_STREAM_WAIT_VALUE_EQ) == CUDA_SUCCESS;
}

bool CudaTransferBarrier::start(TransferFuture future) {
    auto shared_future = std::make_shared<TransferFuture>(std::move(future));
    return start([shared_future] { return shared_future->get(); });
}

bool CudaTransferBarrier::start(std::function<ErrorCode()> operation) {
    if (!operation) return false;
    if (worker_.joinable()) {
        if (!result().has_value()) return false;
        worker_.join();
    }
    started_ = true;
    result_ = ErrorCode::TRANSFER_FAIL;
    std::atomic_ref<uint32_t>(*host_flag_).store(0, std::memory_order_release);
    try {
        worker_ =
            std::thread([this, operation = std::move(operation)]() mutable {
                try {
                    if (cudaSetDevice(device_) == cudaSuccess)
                        result_ = operation();
                } catch (const std::exception& error) {
                    LOG(ERROR)
                        << "CUDA stream transfer failed: " << error.what();
                } catch (...) {
                    LOG(ERROR) << "CUDA stream transfer failed";
                }
                std::atomic_ref<uint32_t>(*host_flag_)
                    .store(1, std::memory_order_release);
            });
    } catch (...) {
        std::atomic_ref<uint32_t>(*host_flag_)
            .store(1, std::memory_order_release);
        return false;
    }
    return true;
}

std::optional<ErrorCode> CudaTransferBarrier::result() const {
    if (!started_ || std::atomic_ref<uint32_t>(*host_flag_)
                             .load(std::memory_order_acquire) == 0)
        return std::nullopt;
    return result_;
}

ErrorCode CudaTransferBarrier::wait() {
    if (!started_) return ErrorCode::INVALID_PARAMS;
    if (worker_.joinable()) worker_.join();
    return result_;
}

bool StartBatchGetWithCudaBarrier(
    std::shared_ptr<Client> client, std::vector<std::string> keys,
    std::unordered_map<std::string, std::vector<Slice>> slices,
    CudaTransferBarrier& barrier) {
    if (!client) return false;
    return barrier.start([client = std::move(client), keys = std::move(keys),
                          slices = std::move(slices)]() mutable {
        auto results = client->BatchGet(keys, slices);
        if (results.size() != keys.size()) return ErrorCode::TRANSFER_FAIL;
        for (const auto& result : results)
            if (!result) return result.error();
        return ErrorCode::OK;
    });
}

}  // namespace mooncake

#endif
