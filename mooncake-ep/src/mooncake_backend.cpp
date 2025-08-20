#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>

namespace mooncake {

constexpr const char* MULTI_DEVICE_ERROR_MSG =
    "Expecting one tensor only but got multiple.";
constexpr const char* SYNC_OP_ERROR_MSG = "Expecting async op but got sync op.";
constexpr const char* REDUCE_OP_ERROR_MSG = "Only support SUM.";
constexpr const char* SPARSE_ERROR_MSG = "Sparse op not supported.";
constexpr const char* REDUCE_DTYPE_ERROR_MSG = "Unsupported reduce dtype: ";

std::string MooncakeBackend::hostIp_ = "127.0.0.1";

MooncakeBackend::MooncakeBackend(
    c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
    c10::intrusive_ptr<MooncakeBackendOptions> options)
    : Backend(rank, size), worker_(&engine_, rank, size) {
    // Get device data
    cudaError err = cudaGetDevice(&device_id_);
    TORCH_CHECK(!err, c10::str("Failed to get device id"));

    // Store broken ranks
    if (options) {
        brokenRanks_ = options->brokenRanks_;
    }

    // Initialize transfer engine
    engine_.init(P2PHANDSHAKE, hostIp_);
    auto transport = engine_.installTransport("rdma", nullptr);
    TORCH_CHECK(transport != nullptr, c10::str("Failed to install transport"));
    auto localRpcMeta = transport->meta()->localRpcMeta();
    std::string localServerName = localRpcMeta.ip_or_host_name + ":" +
                                  std::to_string(localRpcMeta.rpc_port);

    // Register GPU buffers
    constexpr size_t buffer_size = 1u << 29;
    std::string location = "cuda:" + std::to_string(device_id_);
    for (size_t i = 0; i < 2; i++) {
        err = cudaMalloc(&send_buffer_[i], buffer_size);
        TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

        int rc =
            engine_.registerLocalMemory(send_buffer_[i], buffer_size, location);
        TORCH_CHECK(!rc, c10::str("Failed to register local memory"));
    }

    for (size_t i = 0; i < 2; i++) {
        err = cudaMalloc(&recv_buffer_[i], buffer_size);
        TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

        int rc =
            engine_.registerLocalMemory(recv_buffer_[i], buffer_size, location);
        TORCH_CHECK(!rc, c10::str("Failed to register local memory"));
    }

    // Register CPU sync regions
    for (size_t i = 0; i < 2; i++) {
        cpu_sync_send_region_[i] =
            new int32_t[MooncakeWorker::kNumTasks_ * size];
        int rc = engine_.registerLocalMemory(
            cpu_sync_send_region_[i],
            MooncakeWorker::kNumTasks_ * size * sizeof(int32_t),
            kWildcardLocation);
        TORCH_CHECK(!rc, c10::str("Failed to register local memory"));
    }

    for (size_t i = 0; i < 2; i++) {
        cpu_sync_recv_region_[i] =
            new int32_t[MooncakeWorker::kNumTasks_ * size];
        int rc = engine_.registerLocalMemory(
            cpu_sync_recv_region_[i],
            MooncakeWorker::kNumTasks_ * size * sizeof(int32_t),
            kWildcardLocation);
        TORCH_CHECK(!rc, c10::str("Failed to register local memory"));
    }

    // Sync metadata
    store->set("server_name_" + std::to_string(rank), localServerName);

    std::vector<std::string> server_names;
    for (int i = 0; i < size; i++) {
        server_names.push_back(
            store->get_to_str({"server_name_" + std::to_string(i)}));
    }
    worker_.initWorker(server_names);
}

MooncakeBackend::~MooncakeBackend() {
    for (size_t i = 0; i < 2; i++) {
        engine_.unregisterLocalMemory(cpu_sync_send_region_[i]);
        delete[] cpu_sync_send_region_[i];
        engine_.unregisterLocalMemory(cpu_sync_recv_region_[i]);
        delete[] cpu_sync_recv_region_[i];
        engine_.unregisterLocalMemory(send_buffer_[i]);
        cudaFree(send_buffer_[i]);
        engine_.unregisterLocalMemory(recv_buffer_[i]);
        cudaFree(recv_buffer_[i]);
    }
}

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }

c10::intrusive_ptr<c10d::Work> MooncakeBackend::broadcast(
    std::vector<at::Tensor>& tensors, const c10d::BroadcastOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    int64_t root = opts.rootRank + opts.rootTensor;
    bool isRoot = (root == rank_);
    cudaStream_t stream =
        at::cuda::getCurrentCUDAStream(tensor.device().index());
    return worker_.putTask(
        c10d::OpType::BROADCAST, tensorSize, root, stream,
        [&](void* dst) {
            if (isRoot) {
                cudaMemcpyAsync(dst, tensor.data_ptr(), tensorSize,
                                cudaMemcpyHostToDevice, stream);
            }
        },
        [&](void* src) {
            cudaMemcpyAsync(tensor.data_ptr(), src, tensorSize,
                            cudaMemcpyDeviceToHost, stream);
        });
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(opts.reduceOp == c10d::ReduceOp::SUM, REDUCE_OP_ERROR_MSG);
    TORCH_CHECK(opts.sparseIndices == std::nullopt, SPARSE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    cudaStream_t stream =
        at::cuda::getCurrentCUDAStream(tensor.device().index());
    return worker_.putTask(
        c10d::OpType::ALLREDUCE, tensorSize, 0, stream,
        [&](void* dst) {
            cudaMemcpyAsync(dst, tensor.data_ptr(), tensorSize,
                            cudaMemcpyHostToDevice, stream);
        },
        [&](void* src) {
            cudaMemsetAsync(tensor.data_ptr(), 0, tensorSize, stream);
            launchReduceKernel(tensor, src, size_, stream);
        });
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allgather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllgatherOptions& opts) {
    TORCH_CHECK(inputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(outputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto inputTensor = inputTensors.back();
    auto outputTensors_ = outputTensors.back();
    size_t tensorSize = inputTensor.numel() * inputTensor.element_size();
    cudaStream_t stream =
        at::cuda::getCurrentCUDAStream(inputTensor.device().index());
    return worker_.putTask(
        c10d::OpType::ALLGATHER, tensorSize, 0, stream,
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

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_allgather_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::AllgatherOptions& opts) {
    size_t tensorSize = inputBuffer.numel() * inputBuffer.element_size();
    cudaStream_t stream =
        at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
    return worker_.putTask(
        c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, stream,
        [&](void* dst) {
            cudaMemcpyAsync(dst, inputBuffer.data_ptr(), tensorSize,
                            cudaMemcpyHostToDevice, stream);
        },
        [&](void* src) {
            cudaMemcpyAsync(outputBuffer.data_ptr(), src, tensorSize * size_,
                            cudaMemcpyDeviceToHost, stream);
        });
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::alltoall(
    std::vector<at::Tensor>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllToAllOptions& opts) {
    size_t tensorSize =
        inputTensors[0].numel() * inputTensors[0].element_size();
    cudaStream_t stream =
        at::cuda::getCurrentCUDAStream(inputTensors[0].device().index());
    return worker_.putTask(
        c10d::OpType::ALLTOALL, tensorSize, 0, stream,
        [&](void* dst) {
            for (const auto j : c10::irange(inputTensors.size())) {
                cudaMemcpyAsync(dst + j * tensorSize,
                                inputTensors[j].data_ptr(), tensorSize,
                                cudaMemcpyHostToDevice, stream);
            }
        },
        [&](void* src) {
            for (const auto j : c10::irange(outputTensors.size())) {
                cudaMemcpyAsync(outputTensors[j].data_ptr(),
                                src + j * tensorSize, tensorSize,
                                cudaMemcpyDeviceToHost, stream);
            }
        });
}
c10::intrusive_ptr<c10d::Work> MooncakeBackend::barrier(
    const c10d::BarrierOptions& opts) {
    TORCH_CHECK(opts.device.has_value(), "Expected opts.device");
    cudaStream_t stream = at::cuda::getCurrentCUDAStream(opts.device->index());
    return worker_.putTask(
        c10d::OpType::BARRIER, 0, 0, stream, [&](void*) {}, [&](void*) {});
}
}  // namespace mooncake
