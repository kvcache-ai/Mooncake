#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <iostream>
#include <thread>
#include <future>

namespace mooncake {

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
}

MooncakeBackend::~MooncakeBackend() {
    engine_.unregisterLocalMemory(buffer_);
    cudaFree(buffer_);
}

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allgather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllgatherOptions& opts) {
    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));
    return c10::make_intrusive<MooncakeWork>(c10d::OpType::ALLGATHER,
                                             std::move(future));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));
    future->markCompleted(c10::IValue(tensors));
    return c10::make_intrusive<MooncakeWork>(c10d::OpType::ALLREDUCE,
                                             std::move(future));
}
}  // namespace mooncake
