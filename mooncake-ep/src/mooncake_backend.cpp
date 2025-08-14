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
    : Backend(rank, size), worker_(&engine_, rank, size) {
    // Initialize transfer engine
    engine_.init("127.0.0.1:2379", std::to_string(rank));
    auto transport = engine_.installTransport("rdma", nullptr);
    TORCH_CHECK(transport != nullptr, c10::str("Failed to install transport"));

    // Register GPU buffers
    constexpr size_t buffer_size = 1u << 30;
    cudaError err = cudaMalloc(&send_buffer_, buffer_size);
    TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

    int rc = engine_.registerLocalMemory(
        send_buffer_, buffer_size, std::string("cuda:") + std::to_string(rank));
    TORCH_CHECK(!rc, c10::str("Failed to register local memory"));

    err = cudaMalloc(&recv_buffer_, buffer_size);
    TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

    rc = engine_.registerLocalMemory(
        recv_buffer_, buffer_size, std::string("cuda:") + std::to_string(rank));
    TORCH_CHECK(!rc, c10::str("Failed to register local memory"));

    // Register CPU sync regions
    cpu_sync_send_region_ = new int32_t[MooncakeWorker::kNumTasks_ * size];
    rc = engine_.registerLocalMemory(
        cpu_sync_send_region_,
        MooncakeWorker::kNumTasks_ * size * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, c10::str("Failed to register local memory"));

    cpu_sync_recv_region_ = new int32_t[MooncakeWorker::kNumTasks_ * size];
    rc = engine_.registerLocalMemory(
        cpu_sync_recv_region_,
        MooncakeWorker::kNumTasks_ * size * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, c10::str("Failed to register local memory"));

    worker_.initWorker();
}

MooncakeBackend::~MooncakeBackend() {
    engine_.unregisterLocalMemory(cpu_sync_send_region_);
    delete[] cpu_sync_send_region_;
    engine_.unregisterLocalMemory(cpu_sync_recv_region_);
    delete[] cpu_sync_recv_region_;
    engine_.unregisterLocalMemory(send_buffer_);
    cudaFree(send_buffer_);
    engine_.unregisterLocalMemory(recv_buffer_);
    cudaFree(recv_buffer_);
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
    size_t tensorSize = inputTensor.numel() * inputTensor.element_size();
    cudaStream_t stream =
        at::cuda::getCurrentCUDAStream(inputTensor.device().index());
    return worker_.putTask(
        c10d::OpType::ALLGATHER, tensorSize, stream,
        [&](void* dst) {
            cudaMemcpyAsync(dst, inputTensor.data_ptr(), tensorSize,
                            cudaMemcpyHostToDevice, stream);
        },
        [&](void* src) {
            for (const auto j : c10::irange(outputTensors_.size())) {
                cudaMemcpyAsync(outputTensors_[j].data_ptr(),
                                src + j * tensorSize, tensorSize,
                                cudaMemcpyDeviceToHost, stream);
            }
        });
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    cudaStream_t stream =
        at::cuda::getCurrentCUDAStream(tensor.device().index());
    return worker_.putTask(
        c10d::OpType::ALLREDUCE, tensorSize, stream,
        [&](void* dst) {
            cudaMemcpyAsync(dst, tensor.data_ptr(), tensorSize,
                            cudaMemcpyHostToDevice, stream);
        },
        [&](void* src) {
            cudaMemcpyAsync(tensor.data_ptr(), src, tensorSize,
                            cudaMemcpyDeviceToHost, stream);
        });
}
}  // namespace mooncake
