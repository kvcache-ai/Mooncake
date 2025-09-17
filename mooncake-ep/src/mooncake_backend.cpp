#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>

namespace mooncake {

constexpr const char* REGISTER_BUFFER_ERROR_MSG =
    "Failed to register local memory.";
constexpr const char* MULTI_DEVICE_ERROR_MSG =
    "Expecting one tensor only but got multiple.";
constexpr const char* SYNC_OP_ERROR_MSG = "Expecting async op but got sync op.";
constexpr const char* REDUCE_OP_ERROR_MSG = "Only support SUM.";
constexpr const char* SPARSE_ERROR_MSG = "Sparse op not supported.";
constexpr const char* REDUCE_DTYPE_ERROR_MSG = "Unsupported reduce dtype: ";

std::string MooncakeBackend::hostIp_ = "127.0.0.1";
TransferEngine MooncakeBackend::engine_ = TransferEngine(true);
Transport* MooncakeBackend::transport_ = nullptr;
int MooncakeBackend::backendIndex_ = 0;
MooncakeWorker MooncakeBackend::worker_;

MooncakeBackend::MooncakeBackend(
    c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
    c10::intrusive_ptr<MooncakeBackendOptions> options, bool isCpu)
    : Backend(rank, size), isCpu_(isCpu) {
    // Get device data
    int deviceId_;
    cudaError err = cudaGetDevice(&deviceId_);
    TORCH_CHECK(!err, c10::str("Failed to get device id"));

    // Initialize transfer engine
    if (!transport_) {
        engine_.init(P2PHANDSHAKE, hostIp_);
        transport_ = engine_.installTransport("rdma", nullptr);
        TORCH_CHECK(transport_ != nullptr,
                    c10::str("Failed to install transport"));
    }
    auto localRpcMeta = transport_->meta()->localRpcMeta();
    std::string localServerName = localRpcMeta.ip_or_host_name + ":" +
                                  std::to_string(localRpcMeta.rpc_port);

    // Register buffers
    std::string location = "cuda:" + std::to_string(deviceId_);
    if (isCpu) {
        for (size_t i = 0; i < 2; i++) {
            send_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(send_buffer_[i],
                        c10::str("Failed to allocate CPU send buffer"));

            int rc = engine_.registerLocalMemory(send_buffer_[i], kBufferSize,
                                                 location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            recv_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(recv_buffer_[i],
                        c10::str("Failed to allocate CPU recv buffer"));

            int rc = engine_.registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                 location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
    } else {
        for (size_t i = 0; i < 2; i++) {
            err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

            int rc = engine_.registerLocalMemory(send_buffer_[i], kBufferSize,
                                                 location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

            int rc = engine_.registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                 location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
    }

    // Register CPU sync regions
    TORCH_CHECK(size <= kMaxNumRanks, "The number of ranks exceeds the limit.");
    for (size_t i = 0; i < 2; i++) {
        cpu_sync_send_region_[i] = new int32_t[kMaxNumRanks];
        int rc = engine_.registerLocalMemory(
            cpu_sync_send_region_[i], kMaxNumRanks * sizeof(int32_t), location);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    for (size_t i = 0; i < 2; i++) {
        cpu_sync_recv_region_[i] = new int32_t[kMaxNumRanks];
        int rc = engine_.registerLocalMemory(
            cpu_sync_recv_region_[i], kMaxNumRanks * sizeof(int32_t), location);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    // Sync metadata
    store->set("server_name_" + std::to_string(rank), localServerName);

    std::vector<std::string> server_names;
    for (int i = 0; i < size; i++) {
        server_names.push_back(
            store->get_to_str({"server_name_" + std::to_string(i)}));
    }

    meta_.rank = rank;
    meta_.size = size;
    meta_.taskCount = 0;
    cudaHostAlloc(&meta_.activeRanks, kMaxNumRanks * sizeof(bool),
                  cudaHostAllocMapped);
    cudaHostGetDevicePointer(&meta_.activeRanksDevice, meta_.activeRanks, 0);
    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        meta_.activeRanks[i] = true;
    }
    if (options) {
        TORCH_CHECK(options->activeRanks_.dtype() == at::kInt,
                    "activeRanks must be int.");
        if (isCpu) {
            TORCH_CHECK(options->activeRanks_.device().is_cpu(),
                        "activeRanks must be on CPU.");
        } else {
            TORCH_CHECK(options->activeRanks_.device().is_cuda(),
                        "activeRanks must be on CUDA.");
        }
        meta_.activeRanksTensor = options->activeRanks_;
    } else {
        meta_.activeRanksTensor =
            at::ones({size}, torch::dtype(torch::kInt32)
                                 .device(isCpu ? torch::kCPU : torch::kCUDA));
    }
    meta_.engine = &engine_;
    meta_.bufferBaseIndex = backendIndex_ * 8;
    for (int i = 0; i < size_; ++i) {
        auto segment_id = engine_.openSegment(server_names[i]);
        meta_.segmentIDs.emplace_back(segment_id);
        auto segment_desc =
            engine_.getMetadata()->getSegmentDescByID(segment_id, true);
        meta_.segmentDescs.emplace_back(segment_desc);
    }

    // Let the default process group warm up the transfer engine
    if (backendIndex_ == 0) {
        std::vector<TransferRequest> entries;
        for (int i = rank_; i < size_; ++i) {
            entries.push_back(TransferRequest{
                .opcode = TransferRequest::READ,
                .source =
                    (int32_t*)meta_.segmentDescs[rank_]->buffers[4].addr + 1,
                .target_id = meta_.segmentIDs[i],
                .target_offset = meta_.segmentDescs[i]->buffers[6].addr,
                .length = sizeof(int32_t),
            });
        }
        auto batchID = engine_.allocateBatchID(entries.size());
        engine_.submitTransfer(batchID, entries);

        while (true) {
            bool batch_done = true;
            TransferStatus status;
            for (int i = 0; i < size_ - rank_; ++i) {
                engine_.getTransferStatus(batchID, i, status);
                if (status.s != TransferStatusEnum::COMPLETED &&
                    status.s != TransferStatusEnum::FAILED) {
                    batch_done = false;
                    break;
                }
            }
            if (batch_done) {
                break;
            }
        }

        store->set("warmup_done_" + std::to_string(rank_), "1");
        for (int i = 0; i < size_; i++) {
            store->get_to_str("warmup_done_" + std::to_string(i));
        }
    }

    // Increment backend index
    ++backendIndex_;
}

MooncakeBackend::~MooncakeBackend() {
    for (size_t i = 0; i < 2; i++) {
        delete[] cpu_sync_send_region_[i];
        delete[] cpu_sync_recv_region_[i];
        if (isCpu_) {
            free(send_buffer_[i]);
            free(recv_buffer_[i]);
        } else {
            cudaFree(send_buffer_[i]);
            cudaFree(recv_buffer_[i]);
        }
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
    if (isCpu_) {
        return worker_.putTaskCpu(
            c10d::OpType::BROADCAST, tensorSize, root, &meta_,
            [=](void* dst) {
                if (isRoot) {
                    memcpy(dst, tensor.data_ptr(), tensorSize);
                }
            },
            [=](void* src) { memcpy(tensor.data_ptr(), src, tensorSize); });
    } else {
        at::cuda::CUDAStream stream =
            at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::BROADCAST, tensorSize, root, &meta_, stream,
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
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(opts.sparseIndices == std::nullopt, SPARSE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    if (isCpu_) {
        auto numRanks = size_;
        return worker_.putTaskCpu(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_,
            [=](void* dst) { memcpy(dst, tensor.data_ptr(), tensorSize); },
            [=](void* src) {
                memset(tensor.data_ptr(), 0, tensorSize);
                launchReduceCpu(tensor, src, numRanks, opts.reduceOp);
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_, stream,
            [&](void* dst) {
                cudaMemcpyAsync(dst, tensor.data_ptr(), tensorSize,
                                cudaMemcpyHostToDevice, stream);
            },
            [&](void* src) {
                cudaMemsetAsync(tensor.data_ptr(), 0, tensorSize, stream);
                launchReduceKernel(tensor, src, size_, opts.reduceOp,
                                   meta_.activeRanksDevice, stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allgather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllgatherOptions& opts) {
    TORCH_CHECK(inputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(outputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto inputTensor = inputTensors.back();
    auto outputTensors_ = outputTensors.back();
    size_t tensorSize = inputTensor.numel() * inputTensor.element_size();
    if (isCpu_) {
        return worker_.putTaskCpu(
            c10d::OpType::ALLGATHER, tensorSize, 0, &meta_,
            [=](void* dst) { memcpy(dst, inputTensor.data_ptr(), tensorSize); },
            [=](void* src) {
                for (const auto j : c10::irange(outputTensors_.size())) {
                    memcpy(outputTensors_[j].data_ptr(), src + j * tensorSize,
                           tensorSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputTensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLGATHER, tensorSize, 0, &meta_, stream,
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
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_allgather_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::AllgatherOptions& opts) {
    size_t tensorSize = inputBuffer.numel() * inputBuffer.element_size();
    if (isCpu_) {
        auto numRanks = size_;
        return worker_.putTaskCpu(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, &meta_,
            [=](void* dst) { memcpy(dst, inputBuffer.data_ptr(), tensorSize); },
            [=](void* src) {
                memcpy(outputBuffer.data_ptr(), src, tensorSize * numRanks);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, &meta_, stream,
            [&](void* dst) {
                cudaMemcpyAsync(dst, inputBuffer.data_ptr(), tensorSize,
                                cudaMemcpyHostToDevice, stream);
            },
            [&](void* src) {
                cudaMemcpyAsync(outputBuffer.data_ptr(), src,
                                tensorSize * size_, cudaMemcpyDeviceToHost,
                                stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_reduce_scatter_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::ReduceScatterOptions& opts) {
    size_t tensorSize = outputBuffer.numel() * outputBuffer.element_size();
    if (isCpu_) {
        auto numRanks = size_;
        return worker_.putTaskCpu(
            c10d::OpType::REDUCE_SCATTER, tensorSize, 0, &meta_,
            [=](void* dst) {
                memcpy(dst, inputBuffer.data_ptr(), tensorSize * numRanks);
            },
            [=](void* src) {
                memset(outputBuffer.data_ptr(), 0, tensorSize);
                launchReduceCpu(outputBuffer, src, numRanks, opts.reduceOp);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::REDUCE_SCATTER, tensorSize, 0, &meta_, stream,
            [&](void* dst) {
                cudaMemcpyAsync(dst, inputBuffer.data_ptr(), tensorSize * size_,
                                cudaMemcpyHostToDevice, stream);
            },
            [&](void* src) {
                cudaMemsetAsync(outputBuffer.data_ptr(), 0, tensorSize, stream);
                launchReduceKernel(outputBuffer, src, size_, opts.reduceOp,
                                   meta_.activeRanksDevice, stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::alltoall(
    std::vector<at::Tensor>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllToAllOptions& opts) {
    size_t tensorSize =
        inputTensors[0].numel() * inputTensors[0].element_size();
    if (isCpu_) {
        return worker_.putTaskCpu(
            c10d::OpType::ALLTOALL, tensorSize, 0, &meta_,
            [=](void* dst) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    memcpy(dst + j * tensorSize, inputTensors[j].data_ptr(),
                           tensorSize);
                }
            },
            [=](void* src) {
                for (const auto j : c10::irange(outputTensors.size())) {
                    memcpy(outputTensors[j].data_ptr(), src + j * tensorSize,
                           tensorSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputTensors[0].device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLTOALL, tensorSize, 0, &meta_, stream,
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
}
c10::intrusive_ptr<c10d::Work> MooncakeBackend::barrier(
    const c10d::BarrierOptions& opts) {
    TORCH_CHECK(isCpu_, "Barrier is available only for CPU.")
    return worker_.putTaskCpu(
        c10d::OpType::BARRIER, 0, 0, &meta_, [=](void*) {}, [=](void*) {});
}
}  // namespace mooncake
