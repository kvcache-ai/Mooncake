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
bool MooncakeBackend::engineInitialized_ = false;
int MooncakeBackend::backendIndex_ = 0;
MooncakeWorker MooncakeBackend::worker_;

MooncakeBackend::MooncakeBackend(
    c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
    c10::intrusive_ptr<MooncakeBackendOptions> options, bool isCpu)
    : Backend(rank, size), isCpu_(isCpu) {
    // Initialize transfer engine
    if (!engineInitialized_) {
        engine_.init(P2PHANDSHAKE, hostIp_);
        engineInitialized_ = true;
    }
    auto localRpcMeta = engine_.getMetadata()->localRpcMeta();
    std::string localServerName = localRpcMeta.ip_or_host_name + ":" +
                                  std::to_string(localRpcMeta.rpc_port);

    // Register buffers
    if (isCpu) {
        for (size_t i = 0; i < 2; i++) {
            send_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(send_buffer_[i],
                        c10::str("Failed to allocate CPU send buffer"));

            int rc = engine_.registerLocalMemory(send_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            recv_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(recv_buffer_[i],
                        c10::str("Failed to allocate CPU recv buffer"));

            int rc = engine_.registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
    } else {
        for (size_t i = 0; i < 2; i++) {
            cudaError err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

            int rc = engine_.registerLocalMemory(send_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            cudaError err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

            int rc = engine_.registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
    }

    // Register CPU sync regions
    TORCH_CHECK(size <= kMaxNumRanks, "The number of ranks exceeds the limit.");
    for (size_t i = 0; i < 2; i++) {
        cpu_sync_send_region_[i] = new int32_t[kMaxNumRanks];
        int rc = engine_.registerLocalMemory(cpu_sync_send_region_[i],
                                             kMaxNumRanks * sizeof(int32_t),
                                             kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    for (size_t i = 0; i < 2; i++) {
        cpu_sync_recv_region_[i] = new int32_t[kMaxNumRanks];
        int rc = engine_.registerLocalMemory(cpu_sync_recv_region_[i],
                                             kMaxNumRanks * sizeof(int32_t),
                                             kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    warmup_send_region_ = new int32_t[kMaxNumRanks];
    warmup_send_region_[0] = 1;
    int rc = engine_.registerLocalMemory(
        warmup_send_region_, kMaxNumRanks * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);

    warmup_recv_region_ = new int32_t[kMaxNumRanks]{};
    rc = engine_.registerLocalMemory(
        warmup_recv_region_, kMaxNumRanks * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);

    // Sync metadata
    store->set("server_name_" + std::to_string(backendIndex_) + "_" +
                   std::to_string(rank_),
               localServerName);

    int backendIndex = backendIndex_;

    std::thread([this, store, backendIndex] {
        connectionPoller(store, backendIndex);
    }).detach();

    meta_.rank = rank;
    meta_.size = size;
    meta_.taskCount = 0;
    if (isCpu) {
        meta_.activeRanks = new bool[kMaxNumRanks];
    } else {
        cudaHostAlloc(&meta_.activeRanks, kMaxNumRanks * sizeof(bool),
                      cudaHostAllocMapped);
        cudaHostGetDevicePointer(&meta_.activeRanksDevice, meta_.activeRanks,
                                 0);
    }
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
    meta_.store = store;
    meta_.backendIndex = backendIndex_;
    meta_.bufferBaseIndex = backendIndex_ * 10;

    while (nextRankForConnection_ != size_) {
        sleep(1);
    }

    if (options && options->isExtension_) {
        auto key = "extension_task_count_" + std::to_string(backendIndex_) +
                   "_" + std::to_string(rank_);
        while (true) {
            if (store->check({key})) {
                meta_.taskCount = std::atoi((char*)store->get(key).data());
                break;
            }
            sleep(1);
        }
    }

    // Increment backend index
    ++backendIndex_;
}

MooncakeBackend::~MooncakeBackend() {
    if (meta_.activeRanks) {
        if (isCpu_) {
            delete[] meta_.activeRanks;
        } else {
            cudaFreeHost(meta_.activeRanks);
        }
        meta_.activeRanks = nullptr;
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
            [=](void* dst, size_t pos, size_t realSize) {
                if (isRoot) {
                    memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                memcpy((char*)tensor.data_ptr() + pos, src, realSize);
            });
    } else {
        at::cuda::CUDAStream stream =
            at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::BROADCAST, tensorSize, root, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                if (isRoot) {
                    cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos,
                                    realSize, cudaMemcpyHostToDevice, stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                cudaMemcpyAsync((char*)tensor.data_ptr() + pos, src, realSize,
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
        auto numRanks = meta_.size;
        return worker_.putTaskCpu(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                memset((char*)tensor.data_ptr() + pos, 0, realSize);
                launchReduceCpu(tensor, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_.activeRanks);
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyHostToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                stream);
                launchReduceKernel(tensor, pos, realSize, src, meta_.size,
                                   opts.reduceOp, meta_.activeRanksDevice,
                                   stream);
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
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)inputTensor.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors_.size())) {
                    memcpy((char*)outputTensors_[j].data_ptr() + pos,
                           (char*)src + j * realSize, realSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputTensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLGATHER, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputTensor.data_ptr() + pos,
                                realSize, cudaMemcpyHostToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors_.size())) {
                    cudaMemcpyAsync((char*)outputTensors_[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
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
        auto numRanks = meta_.size;
        return worker_.putTaskCpu(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)inputBuffer.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(numRanks)) {
                    memcpy(
                        (char*)outputBuffer.data_ptr() + j * tensorSize + pos,
                        (char*)src + j * realSize, realSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputBuffer.data_ptr() + pos,
                                realSize, cudaMemcpyHostToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(meta_.size)) {
                    cudaMemcpyAsync(
                        (char*)outputBuffer.data_ptr() + j * tensorSize + pos,
                        (char*)src + j * realSize, realSize,
                        cudaMemcpyDeviceToHost, stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_reduce_scatter_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::ReduceScatterOptions& opts) {
    size_t tensorSize = outputBuffer.numel() * outputBuffer.element_size();
    if (isCpu_) {
        auto numRanks = meta_.size;
        return worker_.putTaskCpu(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(numRanks)) {
                    memcpy((char*)dst + j * realSize,
                           (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                           realSize);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                memset((char*)outputBuffer.data_ptr() + pos, 0, realSize);
                launchReduceCpu(outputBuffer, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_.activeRanks);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(meta_.size)) {
                    cudaMemcpyAsync(
                        (char*)dst + j * realSize,
                        (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                        realSize, cudaMemcpyHostToDevice, stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)outputBuffer.data_ptr() + pos, 0,
                                realSize, stream);
                launchReduceKernel(outputBuffer, pos, realSize, src, meta_.size,
                                   opts.reduceOp, meta_.activeRanksDevice,
                                   stream);
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
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    memcpy(dst + j * realSize,
                           (char*)inputTensors[j].data_ptr() + pos, realSize);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors.size())) {
                    memcpy((char*)outputTensors[j].data_ptr() + pos,
                           (char*)src + j * realSize, realSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputTensors[0].device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLTOALL, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    cudaMemcpyAsync(dst + j * realSize,
                                    (char*)inputTensors[j].data_ptr() + pos,
                                    realSize, cudaMemcpyHostToDevice, stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors.size())) {
                    cudaMemcpyAsync((char*)outputTensors[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
                                    cudaMemcpyDeviceToHost, stream);
                }
            });
    }
}
c10::intrusive_ptr<c10d::Work> MooncakeBackend::barrier(
    const c10d::BarrierOptions& opts) {
    TORCH_CHECK(isCpu_, "Barrier is available only for CPU.")
    return worker_.putTaskCpu(
        c10d::OpType::BARRIER, 0, 0, &meta_, [=](void*, size_t, size_t) {},
        [=](void*, size_t, size_t) {});
}

void MooncakeBackend::shutdown() {
    isShutdown_ = true;
    engine_.unregisterLocalMemory(warmup_send_region_);
    engine_.unregisterLocalMemory(warmup_recv_region_);
    delete[] warmup_send_region_;
    delete[] warmup_recv_region_;
    for (size_t i = 0; i < 2; i++) {
        engine_.unregisterLocalMemory(cpu_sync_send_region_[i]);
        engine_.unregisterLocalMemory(cpu_sync_recv_region_[i]);
        engine_.unregisterLocalMemory(send_buffer_[i]);
        engine_.unregisterLocalMemory(recv_buffer_[i]);
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

int MooncakeBackend::getNumSyncedRanks() {
    std::vector<at::Tensor> tensors;
    tensors.emplace_back(torch::tensor(
        nextRankForConnection_,
        torch::dtype(torch::kInt).device(isCpu_ ? torch::kCPU : torch::kCUDA)));
    c10d::AllreduceOptions opts{
        .reduceOp = c10d::ReduceOp::MIN,
    };
    auto work = allreduce(tensors, opts);
    work->wait();
    if (!isCpu_) {
        auto stream =
            at::cuda::getCurrentCUDAStream(tensors[0].device().index());
        cudaStreamSynchronize(stream);
    }
    return tensors[0].cpu().item<int>();
}

void MooncakeBackend::extendGroupSizeTo(int size) {
    LOG(INFO) << rank_ << " extend to " << size;
    meta_.size = size;
    meta_.taskCount = 0;
    // TODO: compatibility with fault-tolerance
    meta_.activeRanksTensor =
        at::ones({size}, torch::dtype(torch::kInt32)
                             .device(isCpu_ ? torch::kCPU : torch::kCUDA));
}

std::vector<bool> MooncakeBackend::getPeerState(const std::vector<int>& ranks) {
    bool activeRanksBackup[kMaxNumRanks];
    while (true) {
        std::vector<int> input;
        for (const int rank : ranks) {
            input.push_back(meta_.peerConnected[rank]);
        }
        for (int i = 0; i < meta_.size; i++) {
            activeRanksBackup[i] = meta_.activeRanks[i];
        }

        std::vector<at::Tensor> tensors;
        tensors.emplace_back(torch::tensor(
            input,
            torch::dtype(torch::kInt).device(isCpu_ ? torch::kCPU : torch::kCUDA)));
        c10d::AllreduceOptions opts{
            .reduceOp = c10d::ReduceOp::MIN,
        };
        auto work = allreduce(tensors, opts);
        work->wait();
        if (!isCpu_) {
            auto stream =
                at::cuda::getCurrentCUDAStream(tensors[0].device().index());
            cudaStreamSynchronize(stream);
        }
        bool activeRanksChanged = false;
        for (int i = 0; i < meta_.size; i++) {
            if (activeRanksBackup[i] != meta_.activeRanks[i]) {
                activeRanksChanged = true;
                break;
            }
        }

        if (!activeRanksChanged) {
            std::vector<bool> output;
            for (int i = 0; i < tensors[0].size(0); ++i) {
                output.push_back(tensors[0].cpu()[i].item<int>() != 0);
            }
            return output;
        }
    }
}

void MooncakeBackend::recoverRanks(const std::vector<int>& ranks) {
    for (const int rank : ranks) {
        TORCH_CHECK(meta_.peerConnected[rank]);
        meta_.activeRanks[rank] = true;
        meta_.store->set("extension_task_count_" +
                             std::to_string(meta_.backendIndex) + "_" +
                             std::to_string(rank),
                         std::to_string(meta_.taskCount));
    }
}

void MooncakeBackend::connectionPoller(c10::intrusive_ptr<::c10d::Store> store,
                                       int backendIndex) {
    while (!isShutdown_) {
        for (int pollingRank = 0; pollingRank <= nextRankForConnection_;
             ++pollingRank) {
            if (meta_.peerConnected[pollingRank]) {
                continue;
            }
            std::string serverNameKey = "server_name_" +
                                        std::to_string(backendIndex) + "_" +
                                        std::to_string(pollingRank);
            try {
                if (!store->check({serverNameKey})) {
                    continue;
                }
            } catch (const std::exception& e) {
                sleep(1);
                continue;
            }
            if (isShutdown_) {
                break;
            }
            auto peerServerName = store->get_to_str(serverNameKey);
            auto segment_id = engine_.openSegment(peerServerName);
            meta_.segmentIDs[pollingRank] = segment_id;
            auto segment_desc =
                engine_.getMetadata()->getSegmentDescByID(segment_id, true);
            meta_.segmentDescs[pollingRank] = segment_desc;

            // Indicates the rank has received metadata from pollingRank
            // TODO: experiment whether the matrix can be safely removed
            store->set("warmup_matrix_" + std::to_string(rank_) + "_" +
                           std::to_string(pollingRank),
                       "1");

            if (backendIndex == 0) {
                if (pollingRank <= rank_) {
                    // Send a pre-flight request to establish connections
                    std::vector<TransferRequest> entries;
                    store->get_to_str("warmup_matrix_" +
                                      std::to_string(pollingRank) + "_" +
                                      std::to_string(rank_));
                    auto batchID = engine_.allocateBatchID(1);
                    engine_.submitTransfer(
                        batchID,
                        {TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = warmup_send_region_,
                            .target_id = meta_.segmentIDs[pollingRank],
                            .target_offset = meta_.segmentDescs[pollingRank]
                                                 ->buffers[9]
                                                 .addr +
                                             rank_ * sizeof(int32_t),
                            .length = sizeof(int32_t),
                        }});

                    while (true) {
                        TransferStatus status;
                        engine_.getTransferStatus(batchID, 0, status);
                        if (status.s == TransferStatusEnum::COMPLETED) {
                            break;
                        } else if (status.s == TransferStatusEnum::FAILED) {
                            LOG(WARNING) << "Warmup request " << rank_ << " -> "
                                         << pollingRank << " failed.";
                            break;
                        }
                    }
                } else {
                    // Wait for the warmup signals
                    while (!warmup_recv_region_[pollingRank]) {
                        sleep(1);
                    }
                }
            }
            meta_.peerConnected[pollingRank] = true;
            if (pollingRank == nextRankForConnection_) {
                ++nextRankForConnection_;
            }
        }
        sleep(1);
    }
}
}  // namespace mooncake
