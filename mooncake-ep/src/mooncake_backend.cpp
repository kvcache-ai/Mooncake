#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>

namespace mooncake {

constexpr const char* MULTI_DEVICE_ERROR_MSG =
    "Expecting one tensor only but got multiple.";
constexpr const char* SYNC_OP_ERROR_MSG = "Expecting async op but got sync op.";

MooncakeBackend::MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store,
                                 int rank, int size,
                                 c10::intrusive_ptr<Options> options)
    : Backend(rank, size) {
    engine_.init("127.0.0.1:2379", std::to_string(rank));
    auto transport = engine_.installTransport("rdma", nullptr);
    TORCH_CHECK(transport != nullptr, c10::str("Failed to install transport"));

    constexpr size_t buffer_size = 1u << 30;
    cudaError err = cudaMalloc(&buffer_, buffer_size);
    TORCH_CHECK(!err, c10::str("Failed to allocate CUDA buffer"));

    int rc = engine_.registerLocalMemory(
        buffer_, buffer_size, std::string("cuda:") + std::to_string(rank));
    TORCH_CHECK(!rc, c10::str("Failed to register local memory"));

    worker_.initWorker();
}

MooncakeBackend::~MooncakeBackend() {
    engine_.unregisterLocalMemory(buffer_);
    cudaFree(buffer_);
}

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allgather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllgatherOptions& opts) {
    TORCH_CHECK(inputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(outputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(opts.asyncOp, SYNC_OP_ERROR_MSG);
    auto inputTensor = inputTensors.back();
    auto outputTensors_ = outputTensors.back();
    at::Tensor outputFlattened = c10d::newLikeFlat(outputTensors_);
    return worker_.putTask(
        c10d::OpType::ALLGATHER, inputTensor.data_ptr(),
        outputFlattened.data_ptr(),
        at::cuda::getCurrentCUDAStream(inputTensor.device().index()), [&] {
            for (const auto j : c10::irange(outputTensors_.size())) {
                outputTensors_[j].copy_(
                    outputFlattened[static_cast<int64_t>(j)], true);
            }
        });
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    return worker_.putTask(
        c10d::OpType::ALLREDUCE, tensor.data_ptr(), tensor.data_ptr(),
        at::cuda::getCurrentCUDAStream(tensor.device().index()), [] {});
}
}  // namespace mooncake
